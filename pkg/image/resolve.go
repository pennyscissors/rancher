package image

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/pkg/errors"
	"github.com/rancher/norman/types/convert"
	v32 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	libhelm "github.com/rancher/rancher/pkg/catalog/helm"
	util "github.com/rancher/rancher/pkg/cluster"
	v3 "github.com/rancher/rancher/pkg/generated/norman/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/settings"
	rketypes "github.com/rancher/rke/types"
	img "github.com/rancher/rke/types/image"
	"gopkg.in/yaml.v2"
)

type OSType int

const (
	Linux OSType = iota
	Windows
	SystemChartsRepoDir = "build/system-charts"
)

func Resolve(image string) string {
	return ResolveWithCluster(image, nil)
}

func ResolveWithCluster(image string, cluster *v3.Cluster) string {
	reg := util.GetPrivateRepoURL(cluster)
	if reg != "" && !strings.HasPrefix(image, reg) {
		//Images from Dockerhub Library repo, we add rancher prefix when using private registry
		if !strings.Contains(image, "/") {
			image = "rancher/" + image
		}
		return path.Join(reg, image)
	}

	return image
}

func getChartVersions(path, rancherVersion string) (libhelm.ChartVersions, error) {
	chartVersions := libhelm.ChartVersions{}
	helm := libhelm.Helm{
		LocalPath: path,
		IconPath:  path,
		Hash:      "",
	}
	index, err := helm.LoadIndex()
	if err != nil {
		return nil, err
	}
	for _, versions := range index.IndexFile.Entries {
		if len(versions) > 0 {
			if strings.Contains(path, SystemChartsRepoDir) {
				versionsInRange, err := getVersionsInRancherMinMaxRange(rancherVersion, versions)
				if err != nil {
					return nil, err
				}
				chartVersions = append(chartVersions, versionsInRange...)
			} else {
				// because versions is sorted in reverse order, the first one will be the latest version
				latestVersion := versions[0]
				chartVersions = append(chartVersions, latestVersion)
			}
		}
	}
	return chartVersions, nil
}

func getVersionsInRancherMinMaxRange(rancherVersion string, versions libhelm.ChartVersions) (libhelm.ChartVersions, error) {
	var chartVersions libhelm.ChartVersions
	for _, v := range versions {
		questions, err := fetchVersionQuestions(v)
		if err != nil {
			return nil, err
		}
		// No ok check because a chart without a rancher min/max version is still valid
		min, _ := questions["rancher_min_version"].(string)
		max, _ := questions["rancher_max_version"].(string)
		if len(min) > 0 {
			rancherSemVer, err := semver.NewVersion(strings.TrimSpace(rancherVersion))
			if err != nil {
				return nil, err
			}
			minSemVer, err := semver.NewVersion(strings.TrimSpace(min))
			if err != nil {
				return nil, err
			}
			if len(max) > 0 {
				maxSemVer, err := semver.NewVersion(strings.TrimSpace(max))
				if err != nil {
					return nil, err
				}
				// If chart has both min and max version, append if the rancher version is within the [min, max] range
				if (rancherSemVer.GreaterThan(minSemVer) || rancherSemVer.Equal(minSemVer)) &&
					(rancherSemVer.LessThan(maxSemVer) || rancherSemVer.Equal(maxSemVer)) {
					chartVersions = append(chartVersions, v)
				}
				continue
			}
			// If chart has a min but no max version, append if the rancher version is within the [min, inf) range
			if rancherSemVer.GreaterThan(minSemVer) || rancherSemVer.Equal(minSemVer) {
				chartVersions = append(chartVersions, v)
			}
		}
	}
	if len(chartVersions) <= 0 {
		// If no chart was appended, append the latest version of it
		chartVersions = append(chartVersions, versions[0])
	}
	return chartVersions, nil
}

func fetchVersionQuestions(version *libhelm.ChartVersion) (map[interface{}]interface{}, error) {
	var questions map[interface{}]interface{}
	for _, path := range version.LocalFiles {
		basename := filepath.Base(path)
		if strings.EqualFold(basename, "questions.yaml") || strings.EqualFold(basename, "questions.yml") {
			data, err := ioutil.ReadFile(path)
			if err != nil {
				return nil, err
			}
			questions = make(map[interface{}]interface{})
			if err := yaml.Unmarshal(data, &questions); err != nil {
				return nil, err
			}
		}
	}
	return questions, nil
}

func pickImagesFromValuesYAML(imagesSet map[string]map[string]bool, chartVersions libhelm.ChartVersions, basePath, path string, info os.FileInfo, osType OSType) error {
	if info.Name() != "values.yaml" {
		return nil
	}
	relPath, err := filepath.Rel(basePath, path)
	if err != nil {
		return err
	}
	var chartNameAndVersion string
	for _, v := range chartVersions {
		if strings.HasPrefix(relPath, v.Dir) {
			chartNameAndVersion = fmt.Sprintf("%s:%s", v.Name, v.Version)
			break
		}
	}
	if chartNameAndVersion == "" {
		// path does not belong to a given chart
		return nil
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	valuesYaml := map[interface{}]interface{}{}
	if err := yaml.Unmarshal(data, &valuesYaml); err != nil {
		return err
	}

	walkthroughMap(valuesYaml, func(inputMap map[interface{}]interface{}) {
		generateImages(chartNameAndVersion, inputMap, imagesSet, osType)
	})
	return nil
}

func generateImages(chartNameAndVersion string, inputMap map[interface{}]interface{}, output map[string]map[string]bool, osType OSType) {
	repo, ok := inputMap["repository"].(string)
	if !ok {
		return
	}
	tag, ok := inputMap["tag"]
	if !ok {
		return
	}
	// distinguish images by os
	switch inputMap["os"] {
	case "windows": // must have indicate `os: windows` if the image is using in Windows cluster
		if osType != Windows {
			return
		}
	default:
		if osType != Linux {
			return
		}
	}
	imageName := fmt.Sprintf("%s:%v", repo, tag)
	addSourceToImage(output, imageName, chartNameAndVersion)
}

func addSourceToImage(imagesSet map[string]map[string]bool, image string, sources ...string) {
	if imagesSet[image] == nil {
		imagesSet[image] = make(map[string]bool)
	}
	for _, source := range sources {
		imagesSet[image][source] = true
	}
}

func walkthroughMap(inputMap map[interface{}]interface{}, walkFunc func(map[interface{}]interface{})) {
	walkFunc(inputMap)
	for _, value := range inputMap {
		if v, ok := value.(map[interface{}]interface{}); ok {
			walkthroughMap(v, walkFunc)
		}
	}
}

func GetImages(systemChartPath, chartPath, rancherVersion string, k3sUpgradeImages, imagesFromArgs []string, rkeSystemImages map[string]rketypes.RKESystemImages, osType OSType) ([]string, []string, error) {
	// fetch images from system charts
	imagesSet := make(map[string]map[string]bool)
	if systemChartPath != "" {
		if err := fetchImagesFromCharts(systemChartPath, rancherVersion, osType, imagesSet); err != nil {
			return nil, nil, errors.Wrap(err, "failed to fetch images from system charts")
		}
	}

	// fetch images from charts
	if chartPath != "" {
		if err := fetchImagesFromCharts(chartPath, rancherVersion, osType, imagesSet); err != nil {
			return nil, nil, errors.Wrap(err, "failed to fetch images from charts")
		}
	}

	// fetch images from system images
	if len(rkeSystemImages) > 0 {
		if err := fetchImagesFromSystem(rkeSystemImages, osType, imagesSet); err != nil {
			return nil, nil, errors.Wrap(err, "failed to fetch images from system images")
		}
	}

	setRequirementImages(osType, imagesSet)

	// set rancher images from args
	setImages("rancher", imagesFromArgs, imagesSet)

	// set images for k3s-upgrade
	setImages("k3sUpgrade", k3sUpgradeImages, imagesSet)

	convertMirroredImages(imagesSet)

	imagesList, imagesAndSourcesList := generateImageAndSourceLists(imagesSet)

	return imagesList, imagesAndSourcesList, nil
}

func setImages(source string, imagesFromArgs []string, imagesSet map[string]map[string]bool) {
	for _, image := range imagesFromArgs {
		addSourceToImage(imagesSet, image, source)
	}
}

func convertMirroredImages(imagesSet map[string]map[string]bool) {
	for image := range imagesSet {
		convertedImage := img.Mirror(image)
		if image == convertedImage {
			continue
		}
		for source, val := range imagesSet[image] {
			if !val {
				continue
			}
			addSourceToImage(imagesSet, convertedImage, source)
		}
		delete(imagesSet, image)
	}
}

func fetchImagesFromCharts(path, rancherVersion string, osType OSType, imagesSet map[string]map[string]bool) error {
	chartVersions, err := getChartVersions(path, rancherVersion)
	if err != nil {
		return errors.Wrapf(err, "failed to get chart and version from %q", path)
	}

	err = filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return pickImagesFromValuesYAML(imagesSet, chartVersions, path, p, info, osType)
	})
	if err != nil {
		return errors.Wrap(err, "failed to pick images from values.yaml")
	}

	return nil
}

func fetchImagesFromSystem(rkeSystemImages map[string]rketypes.RKESystemImages, osType OSType, imagesSet map[string]map[string]bool) error {
	collectionImagesList := []interface{}{
		rkeSystemImages,
	}
	switch osType {
	case Linux:
		collectionImagesList = append(collectionImagesList, v32.ToolsSystemImages)
	}

	images, err := flatImagesFromCollections(collectionImagesList...)
	if err != nil {
		return err
	}

	for _, image := range images {
		addSourceToImage(imagesSet, image, "system")

	}
	return nil
}

func flatImagesFromCollections(cols ...interface{}) (images []string, err error) {
	for _, col := range cols {
		colObj := map[string]interface{}{}
		if err := convert.ToObj(col, &colObj); err != nil {
			return []string{}, err
		}

		images = append(images, fetchImagesFromCollection(colObj)...)
	}
	return images, nil
}

func fetchImagesFromCollection(obj map[string]interface{}) (images []string) {
	for _, v := range obj {
		switch t := v.(type) {
		case string:
			images = append(images, t)
		case map[string]interface{}:
			images = append(images, fetchImagesFromCollection(t)...)
		}
	}
	return images
}

func setRequirementImages(osType OSType, imagesSet map[string]map[string]bool) {
	coreLabel := "core"
	switch osType {
	case Linux:
		addSourceToImage(imagesSet, settings.ShellImage.Get(), coreLabel)
		addSourceToImage(imagesSet, "busybox", coreLabel)
	}
}

func generateImageAndSourceLists(imagesSet map[string]map[string]bool) ([]string, []string) {
	var images, imagesAndSources []string
	// unique
	for image := range imagesSet {
		images = append(images, image)
	}

	// sort
	sort.Slice(images, func(i, j int) bool {
		return images[i] < images[j]
	})

	for _, image := range images {
		imagesAndSources = append(imagesAndSources, fmt.Sprintf("%s %s", image, getSourcesList(imagesSet[image])))
	}

	return images, imagesAndSources
}

func getSourcesList(imageSources map[string]bool) string {
	var sources []string

	for source, val := range imageSources {
		if !val {
			continue
		}
		sources = append(sources, source)
	}
	sort.Strings(sources)
	return strings.Join(sources, ",")
}
