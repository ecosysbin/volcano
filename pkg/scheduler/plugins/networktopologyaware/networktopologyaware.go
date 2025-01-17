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
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName            = "networktopologyaware"
	BaseScore             = 100.0
	TaskSumBaseScore      = 10.0
	ZeroScore             = 0.0
	NetworkTopologyWeight = "weight"
)

type networkTopologyAwarePlugin struct {
	// Arguments given for the plugin
	pluginArguments framework.Arguments
	*hyperNodesTier
}

type hyperNodesTier struct {
	maxTier int
	minTier int
}

func (h *hyperNodesTier) init(hyperNodesSetByTier []int) {
	if len(hyperNodesSetByTier) == 0 {
		return
	}
	h.minTier = hyperNodesSetByTier[0]
	h.maxTier = hyperNodesSetByTier[len(hyperNodesSetByTier)-1]
}

// New function returns prioritizePlugin object
func New(arguments framework.Arguments) framework.Plugin {
	return &networkTopologyAwarePlugin{
		pluginArguments: arguments,
		hyperNodesTier:  &hyperNodesTier{},
	}
}

func (nta *networkTopologyAwarePlugin) Name() string {
	return PluginName
}

func calculateWeight(args framework.Arguments) int {
	weight := 1
	args.GetInt(&weight, NetworkTopologyWeight)
	return weight
}

func (nta *networkTopologyAwarePlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter networkTopologyAwarePlugin plugin ...")
	defer func() {
		klog.V(5).Infof("Leaving networkTopologyAware plugin ...")
	}()

	weight := calculateWeight(nta.pluginArguments)
	nta.hyperNodesTier.init(ssn.HyperNodesTiers)

	hyperNodeFn := func(job *api.JobInfo, hyperNodes map[string][]*api.NodeInfo) (map[string]float64, error) {
		hyperNodeScores := make(map[string]float64)

		jobAllocatedHyperNode := job.PodGroup.GetAnnotations()[api.JobAllocatedHyperNode]
		if jobAllocatedHyperNode == "" {
			for hyperNode := range hyperNodes {
				hyperNodeScores[hyperNode] = ZeroScore
			}
			return hyperNodeScores, nil
		}
		// The job still has remaining tasks to be scheduled, calculate score based on LCAHyperNode tier of the hyperNode and jobAllocatedHyperNode.
		maxScore := ZeroScore
		scoreHyperNode := map[float64][]string{}
		for hyperNode := range hyperNodes {
			score := nta.networkTopologyAwareScore(hyperNode, jobAllocatedHyperNode, ssn.HyperNodes)
			score *= float64(weight)
			hyperNodeScores[hyperNode] = score
			if score >= maxScore {
				maxScore = score
				scoreHyperNode[maxScore] = append(scoreHyperNode[maxScore], hyperNode)
			}
		}
		// Calculate score based on the number of tasks scheduled for the job when max score of hyperNode has more than one.
		if maxScore != ZeroScore && len(scoreHyperNode[maxScore]) > 1 {
			reScoreHyperNodes := scoreHyperNode[maxScore]
			for _, hyperNode := range reScoreHyperNodes {
				taskNumScore := nta.networkTopologyAwareScoreWithTaskNum(hyperNode, job, ssn.RealNodesList)
				taskNumScore *= float64(weight)
				hyperNodeScores[hyperNode] += taskNumScore
			}
		}

		klog.V(1).Infof("networkTopologyAware score is: %v", hyperNodeScores)
		return hyperNodeScores, nil
	}

	nodeFn := func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		nodeScores := make(map[string]float64)
		taskJob := ssn.Jobs[task.Job]

		jobAllocatedHyperNode := taskJob.PodGroup.GetAnnotations()[api.JobAllocatedHyperNode]
		if jobAllocatedHyperNode == "" {
			for _, node := range nodes {
				nodeScores[node.Name] = ZeroScore
			}
			return nodeScores, nil
		}
		// The job still has remaining tasks to be scheduled, calculate score based on LCAHyperNode tier.
		maxScore := ZeroScore
		scoreNodes := map[float64][]string{}
		for _, node := range nodes {
			hyperNode := util.FindHyperNodeOfNode(node.Name, ssn.RealNodesList, ssn.HyperNodesTiers, ssn.HyperNodesSetByTier)
			score := nta.networkTopologyAwareScore(hyperNode, jobAllocatedHyperNode, ssn.HyperNodes)
			score *= float64(weight)
			nodeScores[node.Name] = score
			if score >= maxScore {
				maxScore = score
				scoreNodes[maxScore] = append(scoreNodes[maxScore], node.Name)
			}
		}
		// Calculate score based on the number of tasks scheduled for the job when max score of hyperNode has more than one.
		if maxScore != ZeroScore && len(scoreNodes[maxScore]) > 1 {
			reScoreNodes := scoreNodes[maxScore]
			for _, node := range reScoreNodes {
				hyperNode := util.FindHyperNodeOfNode(node, ssn.RealNodesList, ssn.HyperNodesTiers, ssn.HyperNodesSetByTier)
				taskNumScore := nta.networkTopologyAwareScoreWithTaskNum(hyperNode, taskJob, ssn.RealNodesList)
				taskNumScore *= float64(weight)
				nodeScores[node] += taskNumScore
			}
		}

		klog.V(1).Infof("networkTopologyAware score is: %v", nodeScores)
		return nodeScores, nil
	}

	ssn.AddHyperNodeOrederFn(nta.Name(), hyperNodeFn)
	ssn.AddBatchNodeOrderFn(nta.Name(), nodeFn)
}

func (bp *networkTopologyAwarePlugin) OnSessionClose(ssn *framework.Session) {
}

// networkTopologyAwareScore use the best fit polices during scheduling.

// Goals:
// - The tier of LCAHyperNode of the hyperNode and the job allocatedHyperNode should be as low as possible.
func (nta *networkTopologyAwarePlugin) networkTopologyAwareScore(hyperNodeName, jobAllocatedHyperNode string, hyperNodeMap api.HyperNodeInfoMap) float64 {
	if hyperNodeName == jobAllocatedHyperNode {
		return BaseScore
	}
	LCAHyperNode := hyperNodeMap.GetLCAHyperNode(hyperNodeName, jobAllocatedHyperNode)
	hyperNodeInfo, ok := hyperNodeMap[LCAHyperNode]
	if !ok {
		return ZeroScore
	}
	// Calculate score: (maxTier - LCAhyperNode.tier)/(maxTier - minTier)
	hyperNodeTierScore := BaseScore * scoreHyperNodeWithTier(hyperNodeInfo.Tier(), nta.minTier, nta.maxTier)
	return hyperNodeTierScore
}

// Goals:
// - Tasks under a job should be scheduled to one hyperNode as much as possible.
func (nta *networkTopologyAwarePlugin) networkTopologyAwareScoreWithTaskNum(hyperNodeName string, job *api.JobInfo, realNodesList map[string][]*api.NodeInfo) float64 {
	taskNum := util.FindJobTaskNumOfHyperNode(hyperNodeName, job, realNodesList)
	taskNumScore := ZeroScore
	if len(job.Tasks) > 0 {
		// Calculate score: taskNum/allTaskNum
		taskNumScore = TaskSumBaseScore * scoreHyperNodeWithTaskNum(taskNum, len(job.Tasks))
	}
	return taskNumScore
}

func scoreHyperNodeWithTier(tier int, minTier int, maxTier int) float64 {
	// Use tier to calculate scores and map the original score to the range between 0 and 1.
	if minTier == maxTier {
		return ZeroScore
	}
	return float64(maxTier-tier) / float64(maxTier-minTier)
}

func scoreHyperNodeWithTaskNum(taskNum int, allTaskNum int) float64 {
	// Calculate task distribution rate as score and map the original score to the range between 0 and 1.
	if allTaskNum == 0 {
		return ZeroScore
	}
	return float64(taskNum) / float64(allTaskNum)
}
