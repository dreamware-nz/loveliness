## 1. Core: coarsen helper

- [ ] 1.1 Add `Coarsen` function to `pkg/analytics/leiden/leiden.go` that takes a graph and partition, returns a coarser graph
- [ ] 1.2 Write tests for `Coarsen`: basic aggregation, empty partition, single community, disconnected graph

## 2. Plugin: hierarchical mode in leiden.go

- [ ] 2.1 Add `hierarchical` and `depth` param parsing in `Compute`
- [ ] 2.2 Implement the recursive loop: run Leiden → coarsen → repeat for `depth` iterations
- [ ] 2.3 Collect per-level results (gamma, modularity, size_histogram, optional assignments)
- [ ] 2.4 Add context cancellation check between levels
- [ ] 2.5 Handle early termination: if a level has only one community, stop and return completed levels

## 3. Plugin: tests

- [ ] 3.1 Test hierarchical mode on a known graph (two cliques connected by a bridge — should find the two cliques at level 1)
- [ ] 3.2 Test configurable depth (depth=1 matches single-gamma output)
- [ ] 3.3 Test include_assignments: true produces assignments on every level
- [ ] 3.4 Test include_assignments: false omits assignments
- [ ] 3.5 Test context cancellation mid-sweep
- [ ] 3.6 Test early termination on single-community level

## 4. Wire format validation

- [ ] 4.1 Verify the `levels` array shape matches the design doc spec
- [ ] 4.2 Verify backward compatibility: non-hierarchical calls return unchanged shape
- [ ] 4.3 Run `go test -race -count=1 ./pkg/analytics/...`

## 5. Documentation

- [ ] 5.1 Add hierarchical mode to the Leiden plugin doc comments
- [ ] 5.2 Add a usage example in `spike/analytics-howto/` or the existing howto doc
- [ ] 5.3 Update the README docs table if the howto doc is new
