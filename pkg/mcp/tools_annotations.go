package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// AnnotationExampleInput is the input shape for examples on
// set_annotation. Mirrors AnnotationExample.
type AnnotationExampleInput struct {
	Title       string         `json:"title,omitempty" jsonschema:"Optional short label, e.g. 'find by name'."`
	Query       string         `json:"query" jsonschema:"Cypher template, typically using $name parameter placeholders."`
	Params      map[string]any `json:"params,omitempty" jsonschema:"Example parameter bindings the LLM can copy."`
	Explanation string         `json:"explanation,omitempty" jsonschema:"What the query does, in human terms."`
}

// SetAnnotationInput is the input for set_annotation.
type SetAnnotationInput struct {
	Target      string                   `json:"target" jsonschema:"Schema target. One of: 'cluster', 'db:<name>', 'db:<name>/table:<name>', 'db:<name>/table:<name>/property:<name>', 'db:<name>/edge:<name>', 'db:<name>/edge:<name>/property:<name>', 'db:<name>/saved_query:<id>'."`
	Description string                   `json:"description,omitempty" jsonschema:"Free-form description of this element. The most important field — this is what an LLM reads to understand the schema."`
	Examples    []AnnotationExampleInput `json:"examples,omitempty" jsonschema:"Parameterized query templates that demonstrate how to use this element."`
	Tags        []string                 `json:"tags,omitempty" jsonschema:"Freeform tags (e.g. 'pii', 'core', 'deprecated')."`
	Extra       map[string]string        `json:"extra,omitempty" jsonschema:"Arbitrary string key/value pairs."`
}

// GetAnnotationInput is the input for get_annotation.
type GetAnnotationInput struct {
	Target string `json:"target" jsonschema:"Schema target to look up. See set_annotation for the format."`
}

// DeleteAnnotationInput is the input for delete_annotation.
type DeleteAnnotationInput struct {
	Target string `json:"target" jsonschema:"Schema target to delete."`
}

// ListAnnotationsInput is the input for list_annotations.
type ListAnnotationsInput struct {
	Prefix string `json:"prefix,omitempty" jsonschema:"Optional target prefix to filter by, e.g. 'db:default/' to list everything in that database."`
}

// AnnotationOutput is the response for get_annotation / set_annotation.
type AnnotationOutput struct {
	Annotation Annotation `json:"annotation"`
}

// ListAnnotationsOutput is the response for list_annotations.
type ListAnnotationsOutput struct {
	Annotations []Annotation `json:"annotations"`
}

// AnnotatedTable is a schema table joined with its annotation (if any).
type AnnotatedTable struct {
	NodeTable  *NodeTable  `json:"node_table,omitempty"`
	EdgeTable  *EdgeTable  `json:"edge_table,omitempty"`
	Annotation *Annotation `json:"annotation,omitempty"`
}

// DescribeSchemaInput is the input for describe_schema.
type DescribeSchemaInput struct {
	Database string `json:"database,omitempty" jsonschema:"Database name (default 'default'). Used to construct annotation targets like 'db:<database>/table:<name>'."`
}

// DescribeSchemaOutput joins the schema with annotations in a single
// payload. The cluster annotation (target='cluster') and database
// annotation (target='db:<database>') are surfaced separately so the
// LLM can read top-level descriptions in one turn.
type DescribeSchemaOutput struct {
	Cluster    *Annotation      `json:"cluster,omitempty"`
	Database   *Annotation      `json:"database,omitempty"`
	NodeTables []AnnotatedTable `json:"node_tables"`
	EdgeTables []AnnotatedTable `json:"edge_tables"`
}

// DeleteAnnotationOutput is the response for delete_annotation.
type DeleteAnnotationOutput struct {
	Status string `json:"status"`
	Target string `json:"target"`
}

func registerAnnotationTools(s *mcp.Server, c *Client, cache *schemaCache, readonly bool) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_annotations",
		Title:       "List schema annotations",
		Description: "Return all annotations attached to schema elements (descriptions, query examples, tags). Pass `prefix` to filter, e.g. 'db:default/' for everything in that database. Annotations are the LLM's primary source for what tables and edges mean — read them before writing queries.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListAnnotationsInput) (*mcp.CallToolResult, ListAnnotationsOutput, error) {
		out, err := c.ListAnnotations(ctx, in.Prefix)
		if err != nil {
			return toolError(err), ListAnnotationsOutput{}, nil
		}
		return nil, ListAnnotationsOutput{Annotations: out}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_annotation",
		Title:       "Read a schema annotation",
		Description: "Return the annotation attached to a specific schema target (description, query examples, tags). Returns an empty annotation if none is set.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in GetAnnotationInput) (*mcp.CallToolResult, AnnotationOutput, error) {
		if strings.TrimSpace(in.Target) == "" {
			return toolError(fmt.Errorf("target is required")), AnnotationOutput{}, nil
		}
		got, err := c.GetAnnotation(ctx, in.Target)
		if err != nil {
			return toolError(err), AnnotationOutput{}, nil
		}
		if got == nil {
			return nil, AnnotationOutput{Annotation: Annotation{Target: in.Target}}, nil
		}
		return nil, AnnotationOutput{Annotation: *got}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "describe_schema",
		Title:       "Schema with annotations joined",
		Description: "Return the full schema (node + edge tables) joined with each element's annotation in a single payload. This is the highest-leverage starting point for an LLM: it shows table shape, column types, and the human-written description and example queries together. Cluster- and database-level annotations are surfaced at the top.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in DescribeSchemaInput) (*mcp.CallToolResult, DescribeSchemaOutput, error) {
		db := strings.TrimSpace(in.Database)
		if db == "" {
			db = "default"
		}
		schemaOut, err := cache.get(ctx, c)
		if err != nil {
			return toolError(err), DescribeSchemaOutput{}, nil
		}
		// Pull all annotations once and index by target. One round-trip
		// is cheaper than N+M GETs for large schemas.
		all, err := c.ListAnnotations(ctx, "")
		if err != nil {
			return toolError(err), DescribeSchemaOutput{}, nil
		}
		idx := make(map[string]*Annotation, len(all))
		for i := range all {
			idx[all[i].Target] = &all[i]
		}
		out := DescribeSchemaOutput{
			Cluster:    idx["cluster"],
			Database:   idx["db:"+db],
			NodeTables: make([]AnnotatedTable, 0, len(schemaOut.NodeTables)),
			EdgeTables: make([]AnnotatedTable, 0, len(schemaOut.EdgeTables)),
		}
		for i := range schemaOut.NodeTables {
			t := schemaOut.NodeTables[i]
			out.NodeTables = append(out.NodeTables, AnnotatedTable{
				NodeTable:  &t,
				Annotation: idx["db:"+db+"/table:"+t.Name],
			})
		}
		for i := range schemaOut.EdgeTables {
			t := schemaOut.EdgeTables[i]
			out.EdgeTables = append(out.EdgeTables, AnnotatedTable{
				EdgeTable:  &t,
				Annotation: idx["db:"+db+"/edge:"+t.Name],
			})
		}
		return nil, out, nil
	})

	if readonly {
		return
	}

	mcp.AddTool(s, &mcp.Tool{
		Name:        "set_annotation",
		Title:       "Attach an annotation to a schema element",
		Description: "Write the description, query examples, and tags for a schema element. Latest-wins: a SET on an existing target replaces the entire body. Targets follow 'cluster' / 'db:<name>' / 'db:<name>/table:<name>' / etc. — see the input schema for the full list. Use this to teach the system what a table or edge means and how to query it.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in SetAnnotationInput) (*mcp.CallToolResult, AnnotationOutput, error) {
		if strings.TrimSpace(in.Target) == "" {
			return toolError(fmt.Errorf("target is required")), AnnotationOutput{}, nil
		}
		a := Annotation{
			Target:      in.Target,
			Description: in.Description,
			Tags:        in.Tags,
			Extra:       in.Extra,
		}
		for _, ex := range in.Examples {
			a.Examples = append(a.Examples, AnnotationExample{
				Title:       ex.Title,
				Query:       ex.Query,
				Params:      ex.Params,
				Explanation: ex.Explanation,
			})
		}
		if err := c.SetAnnotation(ctx, a); err != nil {
			return toolError(err), AnnotationOutput{}, nil
		}
		// Re-fetch to get the server-stamped UpdatedAt.
		got, err := c.GetAnnotation(ctx, in.Target)
		if err != nil || got == nil {
			return nil, AnnotationOutput{Annotation: a}, nil
		}
		return nil, AnnotationOutput{Annotation: *got}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "delete_annotation",
		Title:       "Delete a schema annotation",
		Description: "Remove the annotation attached to a target. The schema element itself is unaffected.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in DeleteAnnotationInput) (*mcp.CallToolResult, DeleteAnnotationOutput, error) {
		if strings.TrimSpace(in.Target) == "" {
			return toolError(fmt.Errorf("target is required")), DeleteAnnotationOutput{}, nil
		}
		if err := c.DeleteAnnotation(ctx, in.Target); err != nil {
			return toolError(err), DeleteAnnotationOutput{}, nil
		}
		return nil, DeleteAnnotationOutput{Status: "deleted", Target: in.Target}, nil
	})
}
