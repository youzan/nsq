package consistence

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChooseNewLeaderByLeastLoadFactor(t *testing.T) {
	dpm := &DataPlacement{}
	testCases := []struct {
		loadFactors map[string]float64
		newLeader   []string
	}{
		{
			loadFactors: map[string]float64{"a": 1},
			newLeader:   []string{"a"},
		},
		{
			loadFactors: map[string]float64{"a": 96, "b": 35},
			newLeader:   []string{"b"},
		},
		{
			loadFactors: map[string]float64{"a": 35, "b": 35},
			newLeader:   []string{"a", "b"},
		},
		{
			loadFactors: map[string]float64{"a": 98, "b": 56, "c": 63},
			newLeader:   []string{"b"},
		},
		{
			loadFactors: map[string]float64{"a": 98, "b": 56, "c": 56},
			newLeader:   []string{"b", "c"},
		},
		{
			loadFactors: map[string]float64{"a": 56, "b": 56, "c": 56},
			newLeader:   []string{"a", "b", "c"},
		},
	}
	for _, tCase := range testCases {
		newLeader := dpm.chooseNewLeaderByLeastLoadFactor(tCase.loadFactors)
		assert.Containsf(t, tCase.newLeader, newLeader, "not the right leader %s", newLeader)
	}
}
