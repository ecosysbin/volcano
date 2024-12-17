/*
Copyright 2019 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package networktopologyaware

import (
	"math"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
)

const (
	eps = 1e-3
)

func TestArguments(t *testing.T) {
	framework.RegisterPluginBuilder(PluginName, New)
	defer framework.CleanupPluginBuilders()

	arguments := framework.Arguments{}

	builder, ok := framework.GetPluginBuilder(PluginName)
	if !ok {
		t.Fatalf("should have plugin named %s", PluginName)
	}

	plugin := builder(arguments)
	networkTopologyAware, ok := plugin.(*networkTopologyAwarePlugin)
	if !ok {
		t.Fatalf("plugin should be %T, but not %T", networkTopologyAware, plugin)
	}
}

func TestOrderHyperNodes(t *testing.T) {
	n1 := &api.NodeInfo{Name: "s0-n1"}
	n2 := &api.NodeInfo{Name: "s1-n2"}
	n3 := &api.NodeInfo{Name: "s2-n3"}

	tests := []struct {
		uthelper.TestCommonStruct
		arguments framework.Arguments
		expected  map[string]float64
	}{
		{
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:                 "job first scheduler when RootHyperNode is empty",
				Plugins:              map[string]framework.PluginBuilder{PluginName: New},
				HyperNodesListByTier: map[int][]string{1: {"s0", "s1", "s2"}, 2: {"s3", "s4"}, 3: {"s5"}},
				HyperNodes: map[string]sets.Set[string]{
					"s0": sets.New("s0-n1"),
					"s1": sets.New("s1-n2"),
					"s2": sets.New("s2-n3"),
					"s3": sets.New("s0", "s1", "s0-n1", "s1-n2"),
					"s4": sets.New("s2", "s2-n3"),
					"s5": sets.New("s3", "s4", "s0", "s1", "s2", "s0-n1", "s1-n2", "s2-n3"),
				},
				JobInfo: &api.JobInfo{RootHyperNode: ""},
				OrderHyperNodes: map[string][]*api.NodeInfo{
					"s0": {n1},
					"s1": {n2},
					"s2": {n3},
				},
			},
			arguments: framework.Arguments{},
			expected: map[string]float64{
				"s0": 0.833,
				"s1": 0.833,
				"s2": 0.833,
			},
		},
		{
			TestCommonStruct: uthelper.TestCommonStruct{
				Name:                 "one task of job schedulered on s0 when RootHyperNode is s0",
				Plugins:              map[string]framework.PluginBuilder{PluginName: New},
				HyperNodesListByTier: map[int][]string{1: {"s0", "s1", "s2"}, 2: {"s3", "s4"}, 3: {"s5"}},
				HyperNodes: map[string]sets.Set[string]{
					"s0": sets.New("s0-n1"),
					"s1": sets.New("s1-n2"),
					"s2": sets.New("s2-n3"),
					"s3": sets.New("s0", "s1", "s0-n1", "s1-n2"),
					"s4": sets.New("s2", "s2-n3"),
					"s5": sets.New("s3", "s4", "s0", "s1", "s2", "s0-n1", "s1-n2", "s2-n3"),
				},
				JobInfo: &api.JobInfo{RootHyperNode: "s0"},
				OrderHyperNodes: map[string][]*api.NodeInfo{
					"s0": {n1},
					"s1": {n2},
					"s2": {n3},
				},
			},
			arguments: framework.Arguments{},
			expected: map[string]float64{
				"s0": 1.0,
				"s1": 0.666,
				"s2": 0.5,
			},
		},
	}

	trueValue := true
	for i, test := range tests {
		tiers := []conf.Tier{
			{
				Plugins: []conf.PluginOption{
					{
						Name:                   PluginName,
						EnabledNetworkTopology: &trueValue,
						Arguments:              test.arguments,
					},
				},
			},
		}
		ssn := test.RegisterSession(tiers, nil)
		ssn.HyperNodesListByTier = test.HyperNodesListByTier
		ssn.HyperNodesMap = test.HyperNodes
		ssn.HyperNodesTiers = []int{1, 2, 3}

		score, err := ssn.HyperNodeOrderMapFn(test.JobInfo, test.OrderHyperNodes)
		if err != nil {
			t.Errorf("case%d: task %s  has err %v", i, test.Name, err)
			continue
		}
		hyperNodesScore := score[PluginName]
		for hypernode, expected := range test.expected {
			if math.Abs(hyperNodesScore[hypernode]-expected) > eps {
				t.Errorf("case%d: task %s on hypernode %s expect have score %v, but get %v", i, test.Name, hypernode, expected, hyperNodesScore[hypernode])
			}
		}
	}
}
