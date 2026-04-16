pub mod tools;

use crate::db::Database;
use crate::query;
use anyhow::Result;
use rmcp::{
    ErrorData as McpError, ServiceExt,
    handler::server::router::tool::ToolRouter,
    handler::server::wrapper::Parameters,
    model::*,
    tool, tool_router,
    transport::io::stdio,
};
use std::sync::Arc;
use tools::*;

#[derive(Clone)]
pub struct DiskInventoryServer {
    db: Arc<Database>,
    #[allow(dead_code)] // used by #[tool_router] macro
    tool_router: ToolRouter<Self>,
}

#[tool_router(server_handler)]
impl DiskInventoryServer {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db: Arc::clone(&db),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Get high-level disk usage summary with directory breakdown. Best starting point. Sizes reported as both logical (size_bytes/total_size) and on-disk (disk_bytes/total_disk_bytes). When these differ, files may be cloud placeholders, sparse, or compressed.")]
    async fn disk_overview(
        &self,
        Parameters(params): Parameters<DiskOverviewParams>,
    ) -> Result<CallToolResult, McpError> {
        let result = query::query_overview(
            &self.db,
            params.path.as_deref(),
            params.depth.unwrap_or(1).min(3),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        let json = serde_json::to_string_pretty(&result).unwrap();

        let cloud_children: Vec<_> = result.children.iter()
            .filter(|c| c.disk_bytes != c.total_size && c.total_size > 0)
            .collect();

        let mut content = Vec::new();
        if !cloud_children.is_empty() {
            let total_logical: u64 = cloud_children.iter().map(|c| c.total_size).sum();
            let total_disk: u64 = cloud_children.iter().map(|c| c.disk_bytes).sum();
            content.push(Content::text(format!(
                "Note: {} of content in this path has a different on-disk size than logical size \
                 (logical: {}, on-disk: {}). This typically means cloud-only/streamed files, \
                 sparse files, or filesystem compression. Check disk_bytes vs size_bytes fields.",
                crate::models::format_size(total_logical),
                crate::models::format_size(total_logical),
                crate::models::format_size(total_disk),
            )));
        }
        content.push(Content::text(json));
        Ok(CallToolResult::success(content))
    }

    #[tool(description = "Find the largest files or directories. Filter by type, extensions, and age. Reports both logical size (size_bytes) and on-disk size (disk_bytes). Cloud-only files show disk_bytes=0.")]
    async fn find_large_items(
        &self,
        Parameters(params): Parameters<FindLargeItemsParams>,
    ) -> Result<CallToolResult, McpError> {
        let result = query::query_large_items(
            &self.db,
            params.path.as_deref(),
            params.item_type.as_deref().unwrap_or("both"),
            params.min_size_bytes.unwrap_or(0),
            params.limit.unwrap_or(20).min(100),
            params.file_extensions.as_deref(),
            params.older_than_days,
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let json = serde_json::to_string_pretty(&result).unwrap();

        let divergent: Vec<_> = result.items.iter()
            .filter(|i| i.disk_bytes != i.size_bytes && i.size_bytes > 0)
            .collect();

        let mut content = Vec::new();
        if !divergent.is_empty() {
            let count = divergent.len();
            let logical_sum: u64 = divergent.iter().map(|i| i.size_bytes).sum();
            let disk_sum: u64 = divergent.iter().map(|i| i.disk_bytes).sum();
            content.push(Content::text(format!(
                "Note: {} of {} items have different logical vs on-disk sizes \
                 (logical total: {}, on-disk total: {}). \
                 These may be cloud-only files, sparse, or compressed.",
                count, result.items.len(),
                crate::models::format_size(logical_sum),
                crate::models::format_size(disk_sum),
            )));
        }
        content.push(Content::text(json));
        Ok(CallToolResult::success(content))
    }

    #[tool(description = "Break down disk usage by file type/extension.")]
    async fn disk_usage_by_type(
        &self,
        Parameters(params): Parameters<DiskUsageByTypeParams>,
    ) -> Result<CallToolResult, McpError> {
        let result = query::query_usage_by_type(
            &self.db,
            params.path.as_deref(),
            params.limit.unwrap_or(25),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap(),
        )]))
    }

    #[tool(description = "Search files by name pattern, size range, and date range. Reports both logical size (size_bytes) and on-disk size (disk_bytes).")]
    async fn search_files(
        &self,
        Parameters(params): Parameters<SearchFilesParams>,
    ) -> Result<CallToolResult, McpError> {
        let result = query::query_search(
            &self.db,
            Some(&params.name_pattern),
            params.path.as_deref(),
            params.min_size_bytes,
            params.max_size_bytes,
            params.limit.unwrap_or(50),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap(),
        )]))
    }

    #[tool(description = "Check index freshness, active scan progress, and scan status.")]
    async fn scan_status(
        &self,
        Parameters(_params): Parameters<ScanStatusParams>,
    ) -> Result<CallToolResult, McpError> {
        let result = query::query_scan_status_full(&self.db)
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap(),
        )]))
    }

    #[tool(description = "Find reclaimable space: build artifacts, caches, logs, node_modules. Includes safety ratings and cleanup commands.")]
    async fn find_waste(
        &self,
        Parameters(params): Parameters<FindWasteParams>,
    ) -> Result<CallToolResult, McpError> {
        let categories = params.categories.unwrap_or_else(|| vec!["all".to_string()]);
        let min_size = params.min_size_bytes.unwrap_or(50 * 1024 * 1024); // 50MB default
        let config = crate::config::Config::load()
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        let result = crate::waste::detect_waste(
            &self.db,
            params.path.as_deref(),
            &categories,
            min_size,
            &config.waste.disabled_categories,
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap(),
        )]))
    }

    #[tool(description = "Find duplicate files by content hash.")]
    async fn find_duplicates(
        &self,
        Parameters(params): Parameters<FindDuplicatesParams>,
    ) -> Result<CallToolResult, McpError> {
        let result = crate::duplicates::find_duplicates(
            &self.db,
            params.path.as_deref(),
            params.min_size_bytes.unwrap_or(1024 * 1024), // 1MB default
            None, // extensions filter not in params yet
            params.limit.unwrap_or(20),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap(),
        )]))
    }

    #[tool(description = "Show disk usage trends over time.")]
    async fn disk_trends(
        &self,
        Parameters(params): Parameters<DiskTrendsParams>,
    ) -> Result<CallToolResult, McpError> {
        let result = crate::query::query_trends(
            &self.db,
            params.path.as_deref(),
            params.period.as_deref().unwrap_or("week"),
            "absolute_growth",
            params.limit.unwrap_or(20),
        )
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&result).unwrap(),
        )]))
    }
}

/// Start the MCP server on stdio.
pub async fn run_mcp_server(db: Database) -> Result<()> {
    let server = DiskInventoryServer::new(Arc::new(db));
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
