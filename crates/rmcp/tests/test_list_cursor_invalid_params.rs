#![cfg(not(feature = "local"))]
#![cfg(feature = "client")]

use rmcp::{
    ClientHandler, RoleClient, RoleServer, ServerHandler, ServiceError,
    handler::server::router::{prompt::PromptRouter, tool::ToolRouter},
    model::{ClientConfig, ErrorCode, InitializeResult, PaginatedRequestParams, ProtocolVersion},
    prompt_handler,
    service::{RunningService, serve_directly},
    tool_handler,
};
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
struct ListingServer {
    tool_router: ToolRouter<Self>,
    prompt_router: PromptRouter<Self>,
}

impl ListingServer {
    fn new() -> Self {
        Self {
            tool_router: ToolRouter::new(),
            prompt_router: PromptRouter::new(),
        }
    }
}

#[tool_handler(router = self.tool_router)]
#[prompt_handler(router = self.prompt_router)]
impl ServerHandler for ListingServer {}

#[derive(Debug, Clone)]
struct PlainClient;

impl ClientHandler for PlainClient {
    fn get_info(&self) -> ClientConfig {
        let mut info = ClientConfig::default();
        info.protocol_version = ProtocolVersion::V_2025_11_25;
        info
    }
}

type ServerTask = JoinHandle<anyhow::Result<()>>;

fn connect(server: ListingServer) -> (RunningService<RoleClient, PlainClient>, ServerTask) {
    let (server_transport, client_transport) = tokio::io::duplex(4096);
    let client_handler = PlainClient;
    let mut server_peer_info = InitializeResult::default();
    server_peer_info.protocol_version = ProtocolVersion::V_2025_11_25;

    let server = serve_directly::<RoleServer, _, _, _, _>(
        server,
        server_transport,
        Some(client_handler.get_info()),
    );
    let server_task = tokio::spawn(async move {
        server.waiting().await?;
        anyhow::Ok(())
    });

    let client = serve_directly::<RoleClient, _, _, _, _>(
        client_handler,
        client_transport,
        Some(server_peer_info.into()),
    );
    (client, server_task)
}

async fn disconnect(client: RunningService<RoleClient, PlainClient>, server_task: ServerTask) {
    client.cancel().await.expect("client should cancel");
    server_task.await.expect("server task").expect("server");
}

fn cursor(value: &str) -> Option<PaginatedRequestParams> {
    Some(PaginatedRequestParams {
        meta: None,
        cursor: Some(value.to_owned()),
    })
}

#[tokio::test]
async fn tools_list_without_cursor_succeeds() {
    let (client, server_task) = connect(ListingServer::new());
    let result = client.list_tools(None).await;
    disconnect(client, server_task).await;
    let result = result.expect("first page should list");
    assert!(result.next_cursor.is_none());
}

#[tokio::test]
async fn tools_list_rejects_cursor_this_server_never_issued() {
    let (client, server_task) = connect(ListingServer::new());
    let result = client.list_tools(cursor("not-issued-by-this-server")).await;
    disconnect(client, server_task).await;
    match result {
        Err(ServiceError::McpError(error)) => {
            assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
        }
        other => panic!("expected -32602 Invalid params, got {other:?}"),
    }
}

#[tokio::test]
async fn prompts_list_rejects_cursor_this_server_never_issued() {
    let (client, server_task) = connect(ListingServer::new());
    let result = client.list_prompts(cursor("not-issued-by-this-server")).await;
    disconnect(client, server_task).await;
    match result {
        Err(ServiceError::McpError(error)) => {
            assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
        }
        other => panic!("expected -32602 Invalid params, got {other:?}"),
    }
}
