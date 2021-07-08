package image

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	v32 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	kd "github.com/rancher/rancher/pkg/controllers/management/kontainerdrivermetadata"
	rketypes "github.com/rancher/rke/types"
	"github.com/rancher/rke/types/kdm"
	assertlib "github.com/stretchr/testify/assert"
)

func TestFetchImagesFromSystem(t *testing.T) {
	linuxInfo, windowsInfo, err := getTestK8sVersionInfo()
	if err != nil {
		t.Error(err)
	}
	toolsSystemImages := v32.ToolsSystemImages

	bothImages := []string{
		selectFirstEntry(linuxInfo.RKESystemImages).NginxProxy,
	}
	linuxImagesOnly := []string{
		selectFirstEntry(linuxInfo.RKESystemImages).CoreDNS,
		toolsSystemImages.PipelineSystemImages.Jenkins, // from tools
	}
	windowsImagesOnly := []string{
		selectFirstEntry(windowsInfo.RKESystemImages).WindowsPodInfraContainer,
	}

	testCases := []struct {
		caseName                  string
		inputRkeSystemImages      map[string]rketypes.RKESystemImages
		inputOsType               OSType
		outputShouldContainImages []string
		outputShouldNotContain    []string
	}{
		{
			caseName:             "fetch linux images from system images",
			inputRkeSystemImages: linuxInfo.RKESystemImages,
			inputOsType:          Linux,
			outputShouldContainImages: flatStringSlice(
				bothImages,
				linuxImagesOnly,
			),
			outputShouldNotContain: windowsImagesOnly,
		},
		{
			caseName:             "fetch windows images from system images",
			inputRkeSystemImages: windowsInfo.RKESystemImages,
			inputOsType:          Windows,
			outputShouldContainImages: flatStringSlice(
				bothImages,
				windowsImagesOnly,
			),
			outputShouldNotContain: linuxImagesOnly,
		},
	}

	assert := assertlib.New(t)

	for _, cs := range testCases {
		imagesSet := make(map[string]map[string]bool)
		err := fetchImagesFromSystem(cs.inputRkeSystemImages, cs.inputOsType, imagesSet)
		images, imageSources := getImagesAndSourcesLists(imagesSet)
		assert.Nilf(err, "%s, failed to fetch images from system images", cs.caseName)
		assert.Subset(images, cs.outputShouldContainImages, cs.caseName)
		for _, nc := range cs.outputShouldNotContain {
			assert.NotContains(images, nc, cs.caseName)
		}
		for _, source := range imageSources {
			assert.Equal("system", source)
		}
	}
}

func getImagesAndSourcesLists(imagesSet map[string]map[string]bool) ([]string, []string) {
	var images, imageSources []string
	for image, sources := range imagesSet {
		images = append(images, image)
		for source, val := range sources {
			if !val {
				continue
			}
			imageSources = append(imageSources, source)
		}
	}
	return images, imageSources
}

func getTestK8sVersionInfo() (*kd.VersionInfo, *kd.VersionInfo, error) {
	b, err := ioutil.ReadFile(filepath.Join(os.Getenv("HOME"), "bin", "data.json"))
	if err != nil {
		return nil, nil, err
	}
	data, err := kdm.FromData(b)
	if err != nil {
		return nil, nil, err
	}
	l, w := kd.GetK8sVersionInfo(
		kd.RancherVersionDev,
		data.K8sVersionRKESystemImages,
		data.K8sVersionServiceOptions,
		data.K8sVersionWindowsServiceOptions,
		data.K8sVersionInfo,
	)
	return l, w, nil
}

func flatStringSlice(slices ...[]string) []string {
	var ret []string
	for _, s := range slices {
		ret = append(ret, s...)
	}
	return ret
}

func selectFirstEntry(rkeSystemImages map[string]rketypes.RKESystemImages) rketypes.RKESystemImages {
	for _, rkeSystemImage := range rkeSystemImages {
		return rkeSystemImage
	}
	return rketypes.RKESystemImages{}
}
