// Copyright 2025 Open3FS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/open3fs/m3fs/pkg/config"
	"github.com/open3fs/m3fs/pkg/utils"
	"github.com/sirupsen/logrus"
)

// ================ Type Definitions and Constants ================

// Color and style constants
const (
	// Colors
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
	colorPink   = "\033[38;5;219m"

	// Layout
	defaultRowSize           = 8
	defaultDiagramWidth      = 70
	defaultNodeCellWidth     = 16
	defaultServiceBoxPadding = 2
	defaultTotalCellWidth    = 14

	// Concurrency
	maxWorkers = 10
	timeout    = 30 * time.Second

	// Network
	networkTimeout = 5 * time.Second

	// Initial capacities
	initialStringBuilderCapacity = 1024
	initialMapCapacity           = 16
)

// NetworkType constants
const (
	NetworkTypeEthernet = "ethernet"
	NetworkTypeIB       = "ib"
	NetworkTypeRDMA     = "rdma"
)

// ServiceType defines the type of service in the 3fs cluster
type ServiceType string

// ServiceType constants
const (
	ServiceMgmtd      ServiceType = "mgmtd"
	ServiceMonitor    ServiceType = "monitor"
	ServiceStorage    ServiceType = "storage"
	ServiceFdb        ServiceType = "fdb"
	ServiceClickhouse ServiceType = "clickhouse"
	ServiceMeta       ServiceType = "meta"
	ServiceClient     ServiceType = "client"
)

// ServiceConfig defines a service configuration for rendering
type ServiceConfig struct {
	Type  ServiceType
	Name  string
	Color string
}

// StatInfo represents a statistic item in the summary
type StatInfo struct {
	Name  string
	Count int
	Color string
	Width int
}

// ConfigError represents a configuration-related error
type ConfigError struct {
	msg string
}

func (e *ConfigError) Error() string {
	return e.msg
}

// NetworkError represents a network-related error with operation context
type NetworkError struct {
	operation string
	err       error
}

func (e *NetworkError) Error() string {
	return fmt.Sprintf("%s failed: %v", e.operation, e.err)
}

// ServiceError represents a service-related error with service type context
type ServiceError struct {
	serviceType ServiceType
	err         error
}

func (e *ServiceError) Error() string {
	return fmt.Sprintf("service %s error: %v", e.serviceType, e.err)
}

// nodeResult represents the result of node processing
type nodeResult struct {
	index     int
	nodeName  string
	isStorage bool
}

// Compile regex patterns once
var (
	ibSpeedPattern  = regexp.MustCompile(`rate:\s+(\d+)\s+Gb/sec`)
	ethSpeedPattern = regexp.MustCompile(`Speed:\s+(\d+)\s*([GMK]b/?s)`)
)

// ================ Core Struct and Methods ================

// ArchDiagram generates architecture diagrams for m3fs clusters
type ArchDiagram struct {
	cfg          *config.Config
	colorEnabled bool

	// Layout constants
	defaultRowSize    int
	diagramWidth      int
	nodeCellWidth     int
	serviceBoxPadding int
	totalCellWidth    int

	// Service configurations
	serviceConfigs []ServiceConfig

	// Reusable buffers and caches
	stringBuilderPool sync.Pool
	serviceNodesCache sync.Map
	metaNodesCache    sync.Map

	// Concurrency control
	mu sync.RWMutex
}

// NewArchDiagram creates a new ArchDiagram with default configuration
func NewArchDiagram(cfg *config.Config) *ArchDiagram {
	if cfg == nil {
		logrus.Warn("Creating ArchDiagram with nil config")
		cfg = &config.Config{
			Name:        "default",
			NetworkType: NetworkTypeEthernet,
		}
	}

	// Set default values
	cfg = setDefaultConfig(cfg)

	return &ArchDiagram{
		cfg:               cfg,
		colorEnabled:      true,
		defaultRowSize:    defaultRowSize,
		diagramWidth:      defaultDiagramWidth,
		nodeCellWidth:     defaultNodeCellWidth,
		serviceBoxPadding: defaultServiceBoxPadding,
		totalCellWidth:    defaultTotalCellWidth,
		serviceConfigs:    getDefaultServiceConfigs(),
		stringBuilderPool: sync.Pool{
			New: func() any {
				sb := &strings.Builder{}
				sb.Grow(initialStringBuilderCapacity)
				return sb
			},
		},
	}
}

// Generate generates an architecture diagram
func (g *ArchDiagram) Generate() string {
	if g.cfg == nil {
		return "Error: No configuration provided"
	}

	sb := g.getStringBuilder()
	defer g.putStringBuilder(sb)

	clientNodes := g.getServiceNodes(ServiceClient)
	storageNodes := g.getStorageRelatedNodes()
	serviceNodesMap := g.prepareServiceNodesMap(clientNodes)

	networkSpeed := g.getNetworkSpeed()

	g.renderClusterHeader(sb)
	g.renderClientSection(sb, clientNodes)
	g.renderNetworkSection(sb, networkSpeed)
	g.renderStorageSection(sb, storageNodes)
	g.renderSummarySection(sb, serviceNodesMap)

	return sb.String()
}

// ================ Configuration Methods ================

// setDefaultConfig sets default values for configuration
func setDefaultConfig(cfg *config.Config) *config.Config {
	if cfg.Name == "" {
		cfg.Name = "default"
	}
	if cfg.NetworkType == "" {
		cfg.NetworkType = NetworkTypeEthernet
	}
	return cfg
}

// getDefaultServiceConfigs returns default service configurations
func getDefaultServiceConfigs() []ServiceConfig {
	return []ServiceConfig{
		{ServiceStorage, "storage", colorYellow},
		{ServiceFdb, "foundationdb", colorBlue},
		{ServiceMeta, "meta", colorPink},
		{ServiceMgmtd, "mgmtd", colorPurple},
		{ServiceMonitor, "monitor", colorPurple},
		{ServiceClickhouse, "clickhouse", colorRed},
	}
}

// ================ Node Processing Methods ================

// getStorageRelatedNodes returns nodes that are related to storage services
func (g *ArchDiagram) getStorageRelatedNodes() []string {
	if g.cfg == nil {
		logrus.Error("Configuration is nil")
		return []string{"no storage node"}
	}

	serviceNodesCache := &sync.Map{}

	// Create an ordered list of nodes following config order
	allNodes := make([]string, 0, len(g.cfg.Nodes))
	nodeMap := make(map[string]struct{}, len(g.cfg.Nodes))

	// First, add nodes in the order they appear in the config
	for _, node := range g.cfg.Nodes {
		if _, exists := nodeMap[node.Name]; !exists {
			nodeMap[node.Name] = struct{}{}
			allNodes = append(allNodes, node.Name)
		}
	}

	// Then add any nodes from node groups that haven't been added yet
	for _, nodeGroup := range g.cfg.NodeGroups {
		groupNodes := g.expandNodeGroup(&nodeGroup)
		for _, nodeName := range groupNodes {
			if _, exists := nodeMap[nodeName]; !exists {
				nodeMap[nodeName] = struct{}{}
				allNodes = append(allNodes, nodeName)
			}
		}
	}

	if len(allNodes) == 0 {
		logrus.Warn("No nodes found in configuration")
		return []string{"no storage node"}
	}

	resultChan := make(chan *nodeResult, len(allNodes))
	var wg sync.WaitGroup

	// Create a worker pool
	semaphore := make(chan struct{}, maxWorkers)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Process nodes in order
	for i, nodeName := range allNodes {
		wg.Add(1)
		go func(name string, idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					logrus.Errorf("Recovered from panic in node processing: %v", r)
				}
			}()

			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				logrus.Errorf("Timeout while processing node %s", name)
				return
			}

			// Check each service type in order
			for _, svcConfig := range g.serviceConfigs {
				var serviceNodes []string
				if cached, ok := serviceNodesCache.Load(svcConfig.Type); ok {
					serviceNodes = cached.([]string)
				} else {
					if svcConfig.Type == ServiceMeta {
						serviceNodes = g.getMetaNodes()
					} else {
						serviceNodes = g.getServiceNodes(svcConfig.Type)
					}
					serviceNodesCache.Store(svcConfig.Type, serviceNodes)
				}

				if g.isNodeInList(name, serviceNodes) {
					resultChan <- &nodeResult{
						index:     idx,
						nodeName:  name,
						isStorage: true,
					}
					return
				}
			}
			resultChan <- &nodeResult{
				index:     idx,
				nodeName:  name,
				isStorage: false,
			}
		}(nodeName, i)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results and maintain original order
	results := make([]*nodeResult, len(allNodes))
	for result := range resultChan {
		results[result.index] = result
	}

	// Convert ordered results to final slice
	storageNodes := make([]string, 0, len(allNodes))
	for _, result := range results {
		if result != nil && result.isStorage {
			storageNodes = append(storageNodes, result.nodeName)
		}
	}

	if len(storageNodes) == 0 {
		logrus.Warn("No storage nodes found")
		return []string{"no storage node"}
	}

	return storageNodes
}

// getServiceNodes returns nodes for a specific service type with caching
func (g *ArchDiagram) getServiceNodes(serviceType ServiceType) []string {
	g.mu.RLock()
	if g.cfg == nil {
		g.mu.RUnlock()
		logrus.Error("Cannot get service nodes: configuration is nil")
		return []string{}
	}
	g.mu.RUnlock()

	// Check cache first
	if nodes := g.getCachedNodes(serviceType); nodes != nil {
		return nodes
	}

	nodes, nodeGroups := g.getServiceConfig(serviceType)
	serviceNodes, err := g.getNodesForService(nodes, nodeGroups)
	if err != nil {
		logrus.Errorf("Failed to get nodes for service %s: %v", serviceType, err)
		return []string{}
	}

	if serviceType == ServiceClient && len(serviceNodes) == 0 {
		logrus.Debug("No client nodes found, using default-client")
		serviceNodes = []string{"default-client"}
	}

	// Cache the result
	g.serviceNodesCache.Store(serviceType, serviceNodes)
	return serviceNodes
}

// getCachedNodes retrieves nodes from cache
func (g *ArchDiagram) getCachedNodes(serviceType ServiceType) []string {
	if cached, ok := g.serviceNodesCache.Load(serviceType); ok {
		if nodes, ok := cached.([]string); ok {
			return nodes
		}
	}
	return nil
}

// getServiceConfig returns nodes and node groups for a service type
func (g *ArchDiagram) getServiceConfig(serviceType ServiceType) ([]string, []string) {
	switch serviceType {
	case ServiceMgmtd:
		return g.cfg.Services.Mgmtd.Nodes, g.cfg.Services.Mgmtd.NodeGroups
	case ServiceMonitor:
		return g.cfg.Services.Monitor.Nodes, g.cfg.Services.Monitor.NodeGroups
	case ServiceStorage:
		return g.cfg.Services.Storage.Nodes, g.cfg.Services.Storage.NodeGroups
	case ServiceFdb:
		return g.cfg.Services.Fdb.Nodes, g.cfg.Services.Fdb.NodeGroups
	case ServiceClickhouse:
		return g.cfg.Services.Clickhouse.Nodes, g.cfg.Services.Clickhouse.NodeGroups
	case ServiceMeta:
		return g.cfg.Services.Meta.Nodes, g.cfg.Services.Meta.NodeGroups
	case ServiceClient:
		return g.cfg.Services.Client.Nodes, g.cfg.Services.Client.NodeGroups
	default:
		logrus.Errorf("Unknown service type: %s", serviceType)
		return nil, nil
	}
}

// getNodesForService returns nodes for a service with error handling
func (g *ArchDiagram) getNodesForService(nodes []string, nodeGroups []string) ([]string, error) {
	if g.cfg == nil {
		return nil, &ConfigError{msg: "configuration is nil"}
	}

	serviceNodes := make([]string, 0, len(nodes)+len(nodeGroups))
	nodeMap := make(map[string]struct{}, len(nodes)+len(nodeGroups))

	for _, nodeName := range nodes {
		found := false
		for _, node := range g.cfg.Nodes {
			if node.Name == nodeName {
				if _, exists := nodeMap[node.Name]; !exists {
					nodeMap[node.Name] = struct{}{}
					serviceNodes = append(serviceNodes, node.Name)
				}
				found = true
				break
			}
		}
		if !found {
			logrus.Debugf("Node %s not found in configuration", nodeName)
		}
	}

	for _, groupName := range nodeGroups {
		found := false
		for _, nodeGroup := range g.cfg.NodeGroups {
			if nodeGroup.Name == groupName {
				ipList := g.expandNodeGroup(&nodeGroup)
				for _, ip := range ipList {
					if _, exists := nodeMap[ip]; !exists {
						nodeMap[ip] = struct{}{}
						serviceNodes = append(serviceNodes, ip)
					}
				}
				found = true
				break
			}
		}
		if !found {
			logrus.Debugf("Node group %s not found in configuration", groupName)
		}
	}

	return serviceNodes, nil
}

// isNodeInList checks if a node is in a list using a map for O(1) lookup
func (g *ArchDiagram) isNodeInList(nodeName string, nodeList []string) bool {
	if len(nodeList) == 0 {
		return false
	}

	// Use sync.Map for thread-safe caching
	cacheKey := nodeName + ":" + strings.Join(nodeList, ",")
	if cached, ok := g.serviceNodesCache.Load(cacheKey); ok {
		if result, ok := cached.(bool); ok {
			return result
		}
	}

	nodeSet := make(map[string]struct{}, len(nodeList))
	for _, n := range nodeList {
		nodeSet[n] = struct{}{}
	}
	_, exists := nodeSet[nodeName]

	// Cache the result
	g.serviceNodesCache.Store(cacheKey, exists)
	return exists
}

// getMetaNodes returns meta nodes with caching
func (g *ArchDiagram) getMetaNodes() []string {
	if cached, ok := g.metaNodesCache.Load("meta"); ok {
		if nodes, ok := cached.([]string); ok {
			return nodes
		}
	}

	nodes := g.getServiceNodes(ServiceMeta)
	g.metaNodesCache.Store("meta", nodes)
	return nodes
}

// getClientNodes returns client nodes with caching
func (g *ArchDiagram) getClientNodes() []string {
	return g.getServiceNodes(ServiceClient)
}

// ================ Rendering Methods ================

// renderClusterHeader renders the cluster header section
func (g *ArchDiagram) renderClusterHeader(buffer *strings.Builder) {
	fmt.Fprintf(buffer, "Cluster: %s\n%s\n\n",
		g.cfg.Name,
		strings.Repeat("=", g.diagramWidth))
}

// renderClientSection renders the client nodes section
func (g *ArchDiagram) renderClientSection(buffer *strings.Builder, clientNodes []string) {
	g.renderSectionHeader(buffer, "CLIENT NODES:")
	g.renderNodeRow(buffer, clientNodes, g.defaultRowSize, g.renderClientFunc)
	buffer.WriteByte('\n')
	arrowCount := g.calculateArrowCount(len(clientNodes))
	buffer.WriteString(strings.Repeat("  ↓ ", arrowCount))
	buffer.WriteByte('\n')
}

// renderStorageSection renders the storage nodes section
func (g *ArchDiagram) renderStorageSection(buffer *strings.Builder, storageNodes []string) {
	arrowCount := g.calculateArrowCount(len(storageNodes))
	buffer.WriteString(strings.Repeat("  ↓ ", arrowCount))
	buffer.WriteString("\n\n")
	g.renderSectionHeader(buffer, "STORAGE NODES:")
	g.renderNodeRow(buffer, storageNodes, g.defaultRowSize, g.renderStorageFunc)
}

// renderNetworkSection renders the network section
func (g *ArchDiagram) renderNetworkSection(buffer *strings.Builder, networkSpeed string) {
	networkText := fmt.Sprintf(" %s Network (%s) ", g.cfg.NetworkType, networkSpeed)
	rightPadding := g.diagramWidth - 2 - len(networkText)
	buffer.WriteString("╔" + strings.Repeat("═", g.diagramWidth-2) + "╗\n")
	fmt.Fprintf(buffer, "║%s%s%s%s║\n",
		g.getColorCode(colorBlue),
		networkText,
		g.getColorReset(),
		strings.Repeat(" ", rightPadding))
	buffer.WriteString("╚" + strings.Repeat("═", g.diagramWidth-2) + "╝\n")
}

// renderSummarySection renders the summary section
func (g *ArchDiagram) renderSummarySection(buffer *strings.Builder, serviceNodesMap map[ServiceType][]string) {
	buffer.WriteString("\n")
	g.renderSectionHeader(buffer, "CLUSTER SUMMARY:")
	g.renderSummaryStatistics(buffer, serviceNodesMap)
}

// renderBoxBorder renders a box border with specified count
func (g *ArchDiagram) renderBoxBorder(buffer *strings.Builder, count int) {
	for j := 0; j < count; j++ {
		buffer.WriteString("+----------------+ ")
	}
	buffer.WriteByte('\n')
}

// renderNodeRow renders a row of nodes
func (g *ArchDiagram) renderNodeRow(buffer *strings.Builder, nodes []string, rowSize int,
	renderFunc func(buffer *strings.Builder, nodeName string, index int)) {

	if len(nodes) == 0 {
		return
	}

	nodeCount := len(nodes)
	for i := 0; i < nodeCount; i += rowSize {
		end := i + rowSize
		if end > nodeCount {
			end = nodeCount
		}

		g.renderBoxBorder(buffer, end-i)

		for j := i; j < end; j++ {
			nodeName := nodes[j]
			if len(nodeName) > g.nodeCellWidth {
				nodeName = nodeName[:13] + "..."
			}
			fmt.Fprintf(buffer, "|%s%-16s%s| ", g.getColorCode(colorCyan), nodeName, g.getColorReset())
		}
		buffer.WriteByte('\n')

		renderFunc(buffer, "", i)

		g.renderBoxBorder(buffer, end-i)
	}
}

// renderServiceRow renders a row of services
func (g *ArchDiagram) renderServiceRow(buffer *strings.Builder,
	nodes []string, serviceNodes []string, startIndex int, endIndex int,
	serviceName string, color string) {

	for j := startIndex; j < endIndex; j++ {
		nodeName := nodes[j]
		if g.isNodeInList(nodeName, serviceNodes) {
			serviceLabel := "[" + serviceName + "]"
			paddingNeeded := g.totalCellWidth - len(serviceLabel)
			if paddingNeeded < 0 {
				paddingNeeded = 0
			}

			fmt.Fprintf(buffer, "|  %s%s%s%s| ",
				g.getColorCode(color),
				serviceLabel,
				g.getColorReset(),
				strings.Repeat(" ", paddingNeeded))
		} else {
			buffer.WriteString("|                | ")
		}
	}
	buffer.WriteByte('\n')
}

// renderStorageFunc renders storage nodes
func (g *ArchDiagram) renderStorageFunc(buffer *strings.Builder, _ string, startIndex int) {
	storageNodes := g.getStorageRelatedNodes()

	storageCount := len(storageNodes)
	endIndex := startIndex + g.defaultRowSize
	if endIndex > storageCount {
		endIndex = storageCount
	}

	for _, cfg := range g.serviceConfigs {
		var nodes []string
		if cfg.Type == ServiceMeta {
			nodes = g.getMetaNodes()
		} else {
			nodes = g.getServiceNodes(cfg.Type)
		}
		g.renderServiceRow(buffer, storageNodes, nodes, startIndex, endIndex, cfg.Name, cfg.Color)
	}
}

// renderClientFunc renders client nodes
func (g *ArchDiagram) renderClientFunc(buffer *strings.Builder, _ string, startIndex int) {
	clientNodes := g.getServiceNodes(ServiceClient)
	if len(clientNodes) == 0 {
		return
	}

	clientCount := len(clientNodes)
	endIndex := startIndex + g.defaultRowSize
	if endIndex > clientCount {
		endIndex = clientCount
	}

	g.renderServiceRow(buffer, clientNodes, clientNodes, startIndex, endIndex, "hf3fs_fuse", colorGreen)
}

// renderSectionHeader renders a section header
func (g *ArchDiagram) renderSectionHeader(buffer *strings.Builder, title string) {
	fmt.Fprintf(buffer, "%s%s%s\n%s\n",
		g.getColorCode(colorCyan),
		title,
		g.getColorReset(),
		strings.Repeat("-", g.diagramWidth))
}

// calculateArrowCount calculates the number of arrows to display
func (g *ArchDiagram) calculateArrowCount(nodeCount int) int {
	if nodeCount <= 0 {
		return 1
	} else if nodeCount > 15 {
		return 15
	}
	return nodeCount
}

// renderSummaryRow renders a row in the summary section
func (g *ArchDiagram) renderSummaryRow(buffer *strings.Builder, stats []StatInfo) {
	for _, stat := range stats {
		buffer.WriteString(fmt.Sprintf("%s%-"+fmt.Sprintf("%d", stat.Width)+"s%s %-2d  ",
			g.getColorCode(stat.Color), stat.Name+":", g.getColorReset(), stat.Count))
	}
	buffer.WriteString("\n")
}

// renderSummaryStatistics renders the summary statistics
func (g *ArchDiagram) renderSummaryStatistics(buffer *strings.Builder, serviceNodesMap map[ServiceType][]string) {
	nodeHosts := make(map[string]string, len(g.cfg.Nodes))
	for _, node := range g.cfg.Nodes {
		nodeHosts[node.Name] = node.Host
	}

	serviceNodeCounts := make(map[ServiceType]int, len(serviceNodesMap))

	for svcType, nodeList := range serviceNodesMap {
		uniqueIPs := make(map[string]struct{}, len(nodeList))

		for _, nodeName := range nodeList {
			isNodeGroup := false
			for _, nodeGroup := range g.cfg.NodeGroups {
				groupPattern := fmt.Sprintf("%s[%s-%s]", nodeGroup.Name, nodeGroup.IPBegin, nodeGroup.IPEnd)
				if nodeName == groupPattern {
					isNodeGroup = true
					ipList, err := utils.GenerateIPRange(nodeGroup.IPBegin, nodeGroup.IPEnd)
					if err == nil {
						for _, ip := range ipList {
							uniqueIPs[ip] = struct{}{}
						}
					}
					break
				}
			}

			if !isNodeGroup {
				if host, ok := nodeHosts[nodeName]; ok {
					uniqueIPs[host] = struct{}{}
				} else if nodeName == "default-client" || nodeName == "no storage node" {
					uniqueIPs[nodeName] = struct{}{}
				}
			}
		}

		serviceNodeCounts[svcType] = len(uniqueIPs)
	}

	totalNodeCount := g.getTotalActualNodeCount()

	firstRowStats := []StatInfo{
		{Name: "Client Nodes", Count: serviceNodeCounts[ServiceClient], Color: colorGreen, Width: 13},
		{Name: "Storage Nodes", Count: serviceNodeCounts[ServiceStorage], Color: colorYellow, Width: 14},
		{Name: "FoundationDB", Count: serviceNodeCounts[ServiceFdb], Color: colorBlue, Width: 12},
		{Name: "Meta Service", Count: serviceNodeCounts[ServiceMeta], Color: colorPink, Width: 12},
	}
	g.renderSummaryRow(buffer, firstRowStats)

	secondRowStats := []StatInfo{
		{Name: "Mgmtd Service", Count: serviceNodeCounts[ServiceMgmtd], Color: colorPurple, Width: 13},
		{Name: "Monitor Svc", Count: serviceNodeCounts[ServiceMonitor], Color: colorPurple, Width: 14},
		{Name: "Clickhouse", Count: serviceNodeCounts[ServiceClickhouse], Color: colorRed, Width: 12},
		{Name: "Total Nodes", Count: totalNodeCount, Color: colorCyan, Width: 12},
	}
	g.renderSummaryRow(buffer, secondRowStats)
}

// ================ Network Methods ================

// getNetworkSpeed returns the network speed
func (g *ArchDiagram) getNetworkSpeed() string {
	if speed := g.getIBNetworkSpeed(); speed != "" {
		return speed
	}

	if speed := g.getEthernetSpeed(); speed != "" {
		return speed
	}

	return g.getDefaultNetworkSpeed()
}

// getDefaultNetworkSpeed returns the default network speed
func (g *ArchDiagram) getDefaultNetworkSpeed() string {
	switch g.cfg.NetworkType {
	case config.NetworkTypeIB:
		return "50 Gb/sec"
	case config.NetworkTypeRDMA:
		return "100 Gb/sec"
	default:
		return "10 Gb/sec"
	}
}

// getIBNetworkSpeed returns the InfiniBand network speed
func (g *ArchDiagram) getIBNetworkSpeed() string {
	ctx, cancel := context.WithTimeout(context.Background(), networkTimeout)
	defer cancel()

	cmdIB := exec.CommandContext(ctx, "ibstatus")
	output, err := cmdIB.CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			logrus.Error("Timeout while getting IB network speed")
		} else {
			logrus.Debugf("Failed to get IB network speed: %v", err)
		}
		return ""
	}

	matches := ibSpeedPattern.FindStringSubmatch(string(output))
	if len(matches) > 1 {
		return matches[1] + " Gb/sec"
	}

	logrus.Debug("No IB network speed found in ibstatus output")
	return ""
}

// getEthernetSpeed returns the Ethernet network speed
func (g *ArchDiagram) getEthernetSpeed() string {
	interfaceName, err := g.getDefaultInterface()
	if err != nil {
		logrus.Debugf("Failed to get default interface: %v", err)
		return ""
	}
	if interfaceName == "" {
		logrus.Debug("No default interface found")
		return ""
	}

	speed := g.getInterfaceSpeed(interfaceName)
	if speed == "" {
		logrus.Debugf("Failed to get speed for interface %s", interfaceName)
	}
	return speed
}

// getDefaultInterface returns the default network interface
func (g *ArchDiagram) getDefaultInterface() (string, error) {
	cmdIp := exec.Command("sh", "-c", "ip route | grep default | awk '{print $5}'")
	interfaceOutput, err := cmdIp.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get default interface: %w", err)
	}

	interfaceName := strings.TrimSpace(string(interfaceOutput))
	if interfaceName == "" {
		return "", fmt.Errorf("no default interface found")
	}
	return interfaceName, nil
}

// getInterfaceSpeed returns the speed of a network interface
func (g *ArchDiagram) getInterfaceSpeed(interfaceName string) string {
	if interfaceName == "" {
		return ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), networkTimeout)
	defer cancel()

	cmdEthtool := exec.CommandContext(ctx, "ethtool", interfaceName)
	ethtoolOutput, err := cmdEthtool.CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			logrus.Error("Timeout while getting interface speed")
		} else {
			logrus.Debugf("Failed to get interface speed for %s: %v", interfaceName, err)
		}
		return ""
	}

	matches := ethSpeedPattern.FindStringSubmatch(string(ethtoolOutput))
	if len(matches) > 2 {
		return matches[1] + " " + matches[2]
	}

	logrus.Debugf("No speed found in ethtool output for interface %s", interfaceName)
	return ""
}

// ================ Utility Methods ================

// SetColorEnabled enables or disables color output in the diagram
func (g *ArchDiagram) SetColorEnabled(enabled bool) {
	g.colorEnabled = enabled
}

// getColorReset returns the color reset code
func (g *ArchDiagram) getColorReset() string {
	return g.getColorCode(colorReset)
}

// getColorCode returns a color code if colors are enabled
func (g *ArchDiagram) getColorCode(colorCode string) string {
	if !g.colorEnabled {
		return ""
	}
	return colorCode
}

// getStringBuilder gets a strings.Builder from the pool
func (g *ArchDiagram) getStringBuilder() *strings.Builder {
	return g.stringBuilderPool.Get().(*strings.Builder)
}

// putStringBuilder returns a strings.Builder to the pool
func (g *ArchDiagram) putStringBuilder(sb *strings.Builder) {
	sb.Reset()
	g.stringBuilderPool.Put(sb)
}

// expandNodeGroup expands a node group into individual nodes
func (g *ArchDiagram) expandNodeGroup(nodeGroup *config.NodeGroup) []string {
	cacheKey := fmt.Sprintf("%s[%s-%s]", nodeGroup.Name, nodeGroup.IPBegin, nodeGroup.IPEnd)

	if cached, ok := g.serviceNodesCache.Load(cacheKey); ok {
		if nodes, ok := cached.([]string); ok {
			return nodes
		}
	}

	nodeName := fmt.Sprintf("%s[%s-%s]", nodeGroup.Name, nodeGroup.IPBegin, nodeGroup.IPEnd)
	nodes := []string{nodeName}

	// Cache the result
	g.serviceNodesCache.Store(cacheKey, nodes)
	return nodes
}

// getTotalActualNodeCount returns the total number of actual nodes
func (g *ArchDiagram) getTotalActualNodeCount() int {
	uniqueIPs := make(map[string]struct{})

	for _, node := range g.cfg.Nodes {
		uniqueIPs[node.Host] = struct{}{}
	}

	for _, nodeGroup := range g.cfg.NodeGroups {
		ipList, err := utils.GenerateIPRange(nodeGroup.IPBegin, nodeGroup.IPEnd)
		if err != nil {
			continue
		}

		for _, ip := range ipList {
			uniqueIPs[ip] = struct{}{}
		}
	}

	return len(uniqueIPs)
}

// prepareServiceNodesMap prepares a map of service nodes
func (g *ArchDiagram) prepareServiceNodesMap(clientNodes []string) map[ServiceType][]string {
	serviceNodesMap := make(map[ServiceType][]string, len(g.serviceConfigs)+1)

	for _, cfg := range g.serviceConfigs {
		if cfg.Type == ServiceMeta {
			serviceNodesMap[cfg.Type] = g.getMetaNodes()
		} else {
			serviceNodesMap[cfg.Type] = g.getServiceNodes(cfg.Type)
		}
	}

	serviceNodesMap[ServiceClient] = clientNodes

	return serviceNodesMap
}
