package image

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/repo"

	"gopkg.in/yaml.v2"
)

type ResolveCharts interface {
	getChartVersionsFromIndex() (ChartVersions, error)
	filterFunc(chartVersions ChartVersion) (bool, error)
	pickImagesFromAllValues(imagesSet map[string]map[string]bool, chartVersions ChartVersions) error
}

// Wrapper types for libhelm.ChartVersions and repo.ChartVersions
type ChartVersions []*ChartVersion

// Wrapper type for libhelm.ChartVersion and repo.ChartVersion
type ChartVersion struct {
	*repo.ChartVersion
	Dir        string   `json:"-" yaml:"-"`
	LocalFiles []string `json:"-" yaml:"-"`
}

type SystemCharts struct {
	rancherVersion string
	repoPath       string
	osType         OSType
}

type Questions struct {
	RancherMinVersion string `yaml:"rancher_min_version"`
	RancherMaxVersion string `yaml:"rancher_max_version"`
}

// Fetch all images from a charts repository and filter them based on whether each
// chart's specified rancher version constraint satisfies the rancher version tag
func fetchImages(rc ResolveCharts, imagesSet map[string]map[string]bool) error {
	chartVersions, err := rc.getChartVersionsFromIndex()
	if err != nil {
		return errors.Wrapf(err, "failed to get index")
	}
	filteredChartVersions, err := filterChartVersions(rc, chartVersions)
	if err != nil {
		return errors.Wrapf(err, "failed to filter chart versions")
	}
	err = rc.pickImagesFromAllValues(imagesSet, filteredChartVersions)
	if err != nil {
		return errors.Wrap(err, "failed to pick images from values file")
	}
	return nil
}

// Filter a slice of chartVersion based on whether each chart's specified rancher version constraint satisfies the rancher version tag
func filterChartVersions(rc ResolveCharts, chartVersions ChartVersions) (ChartVersions, error) {
	var filteredChartVersions ChartVersions
	for _, version := range chartVersions {
		addToFiltered, err := rc.filterFunc(*version)
		if err != nil {
			logrus.Info(err)
			continue
		}
		if addToFiltered {
			filteredChartVersions = append(filteredChartVersions, version)
		}
	}
	return filteredChartVersions, nil
}

// Get all system charts from the virtual index's entries
func (sc SystemCharts) getChartVersionsFromIndex() (ChartVersions, error) {
	if sc.repoPath == "" {
		return nil, errors.New("invalid path to system-charts repository")
	}
	helm := libhelm.Helm{
		LocalPath: sc.repoPath,
		IconPath:  sc.repoPath,
		Hash:      "",
	}
	virtualIndex, err := helm.LoadIndex()
	if err != nil {
		return nil, err
	}
	// Convert helm.ChartValues to ChartVersion wrapper type
	var chartVersions ChartVersions
	for _, versions := range virtualIndex.IndexFile.Entries {
		for _, version := range versions {
			chartVersions = append(chartVersions, &ChartVersion{
				ChartVersion: &repo.ChartVersion{
					Metadata: &chart.Metadata{
						Name:    version.Name,
						Version: version.Version,
					},
				},
				Dir:        version.Dir,
				LocalFiles: version.LocalFiles,
			})
		}
	}
	return chartVersions, nil
}

// Filter a system chart based on whether the rancher version constraint in its questions file satisfies the rancher version tag
func (sc SystemCharts) filterFunc(chartVersion ChartVersion) (bool, error) {
	questions, err := decodeQuestions(filepath.Join(sc.repoPath, chartVersion.Dir))
	if err != nil {
		return false, err
	}
	constraintStr := minMaxToConstraintStr(questions.RancherMinVersion, questions.RancherMaxVersion)
	isInRange, err := IsRancherVersionInRange(sc.rancherVersion, constraintStr)
	if err != nil {
		return false, err
	}
	return isInRange, nil
}

// Pick all images from all the values files in a slice of system charts
func (sc SystemCharts) pickImagesFromAllValues(imagesSet map[string]map[string]bool, chartVersions ChartVersions) error {
	for _, version := range chartVersions {
		for _, file := range version.LocalFiles {
			if !isValuesFile(file) {
				continue
			}
			values, err := decodeValues(file)
			if err != nil {
				return err
			}
			chartNameAndVersion := fmt.Sprintf("%s:%s", version.Name, version.Version)
			if err = pickImagesFromValuesMap(imagesSet, values, chartNameAndVersion, sc.osType); err != nil {
				return err
			}
		}
	}
	return nil
}

// Pick all images from a values map
func pickImagesFromValuesMap(imagesSet map[string]map[string]bool, values map[interface{}]interface{}, chartNameAndVersion string, osType OSType) error {
	walkMap(values, func(inputMap map[interface{}]interface{}) {
		repository, ok := inputMap["repository"].(string)
		if !ok {
			return
		}
		tag, ok := inputMap["tag"].(string)
		if !ok {
			return
		}
		imageName := fmt.Sprintf("%s:%v", repository, tag)
		// By default, images are added to the generic images list ("linux"). For Windows and multi-OS
		// images to be considered, they must use a comma-delineated list (e.g. "os: windows",
		// "os: windows,linux", and "os: linux,windows").
		if osList, ok := inputMap["os"].(string); ok {
			for _, os := range strings.Split(osList, ",") {
				switch strings.TrimSpace(strings.ToLower(os)) {
				case "windows":
					if osType == Windows {
						addSourceToImage(imagesSet, imageName, chartNameAndVersion)
						return
					}
				case "linux":
					if osType == Linux {
						addSourceToImage(imagesSet, imageName, chartNameAndVersion)
						return
					}
				}
			}
		} else {
			if inputMap["os"] != nil {
				errors.Errorf("Field 'os:' for image %s contains neither a string nor nil", imageName)
			}
			if osType == Linux {
				addSourceToImage(imagesSet, imageName, chartNameAndVersion)
			}
		}
	})
	return nil
}

// Walk a map and execute the given walk function for each node
func walkMap(data interface{}, walkFunc func(map[interface{}]interface{})) {
	if inputMap, isMap := data.(map[interface{}]interface{}); isMap {
		// Run the walkFunc on the root node and each child node
		walkFunc(inputMap)
		for _, value := range inputMap {
			walkMap(value, walkFunc)
		}
	} else if inputList, isList := data.([]interface{}); isList {
		// Run the walkFunc on each element in the root node, ignoring the root itself
		for _, elem := range inputList {
			walkMap(elem, walkFunc)
		}
	}
}

// Convert min and max Rancher version strings to a constraint string
func minMaxToConstraintStr(min, max string) string {
	if min != "" && max != "" {
		return fmt.Sprintf("%s - %s", min, max)
	}
	if min != "" {
		return fmt.Sprintf(">= %s", min)
	}
	if max != "" {
		return fmt.Sprintf("<= %s", max)
	}
	return ""
}

// Check if a given Rancher version satisfies a given constraint range (E.g ">=2.5.0 <=2.6")
func IsRancherVersionInRange(rancherVersion, constraintStr string) (bool, error) {
	if constraintStr == "" {
		return false, errors.Errorf("Invalid constraint string: \"%s\"", constraintStr)
	}
	rancherSemVer, err := semver.NewVersion(rancherVersion)
	if err != nil {
		return false, err
	}
	// Removing the pre-release because the semver package will not consider a rancherVersion with a
	// pre-releases unless the versions in the constraintStr has pre-releases as well.
	// For example: rancherVersion "2.5.7-rc1" and constraint "2.5.6 - 2.5.8" will return false because
	// there is no pre-release in the constraint "2.5.6 - 2.5.8" (This behavior is intentional).
	rancherSemVerNoPreRelease, err := rancherSemVer.SetPrerelease("")
	if err != nil {
		return false, err
	}
	constraint, err := semver.NewConstraint(constraintStr)
	if err != nil {
		return false, err
	}
	return constraint.Check(&rancherSemVerNoPreRelease), nil
}

// Decode the questions of a given chart version
func decodeQuestions(versionDir string) (Questions, error) {
	var questions Questions
	err := decodeYAML(filepath.Join(versionDir, "questions.yaml"), &questions)
	if err != nil || questions == (Questions{}) {
		err = decodeYAML(filepath.Join(versionDir, "questions.yml"), &questions)
		if err != nil {
			return Questions{}, errors.Errorf("No questions file found in %s", versionDir)
		}
		if questions == (Questions{}) {
			logrus.Infof("questions file in %s is empty", versionDir)
			return Questions{}, nil
		}
	}
	return questions, nil
}

// Decode the values of a given values file
func decodeValues(path string) (map[interface{}]interface{}, error) {
	var values map[interface{}]interface{}
	if err := decodeYAML(path, &values); err != nil {
		return nil, err
	}
	return values, nil
}

// Decode a yaml file
func decodeYAML(input string, target interface{}) error {
	data, err := ioutil.ReadFile(input)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, target)
}
