//! Alibaba Qwen API client implementation.
//!
//! This module provides a client implementation for communicating with Alibaba's DashScope API,
//! specifically designed to work with Qwen language models. It supports both streaming
//! and non-streaming interactions, handling all aspects of API communication including:
//!
//! - Authentication and request signing
//! - Message formatting and serialization
//! - Response parsing and deserialization
//! - Error handling and type conversion
//! - Streaming response processing
//!
//! # Main Components
//!
//! - [`QwenClient`]: The main client struct for making API requests
//! - [`QwenResponse`]: Represents the structured response from the API
//! - [`StreamEvent`]: Represents different types of events in streaming responses

use crate::{
    error::{ApiError, Result},
    models::{ApiConfig, Message, Role},
};
use futures::Stream;
use futures::StreamExt;
use reqwest::{header::HeaderMap, Client};
use serde::{Deserialize, Serialize};
use serde_json;
use std::{collections::HashMap, pin::Pin};

pub(crate) const QWEN_API_URL: &str =
    "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions";
const DEFAULT_MODEL: &str = "qwen-plus";

/// Client for interacting with Alibaba's Qwen models.
#[derive(Debug)]
pub struct QwenClient {
    pub(crate) client: Client,
    api_token: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct QwenResponse {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Usage,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Choice {
    pub index: i32,
    pub message: Message,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct QwenRequest {
    messages: Vec<QwenMessage>,
    stream: bool,

    #[serde(flatten)]
    additional_params: serde_json::Value,
}

// Event types for streaming responses
#[derive(Debug, Deserialize)]
#[serde(tag = "data")]
#[allow(unused)]
pub enum StreamEvent {
    #[serde(rename = "data")]
    Message {
        id: String,
        object: String,
        created: i64,
        model: String,
        choices: Vec<StreamChoice>,
        usage: Option<Usage>,
        // service_tier: Option<String>,
        system_fingerprint: Option<String>,
    },
    #[serde(rename = "NONE")]
    None,
    // #[serde(rename = "error")]
    // Error { error: StreamError },
}
#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct StreamChoice {
    pub index: i32,
    pub delta: QwenMessage,
    pub finish_reason: Option<String>,
    pub logprobs: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct QwenMessage {
    pub role: Option<String>,
    pub content: Option<String>,
}
#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct StreamError {
    pub message: String,
    pub code: String,
}

impl QwenClient {
    pub fn new(api_token: String) -> Self {
        Self {
            client: Client::new(),
            api_token,
        }
    }

    pub(crate) fn build_headers(
        &self,
        custom_headers: Option<&HashMap<String, String>>,
    ) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            format!("Bearer {}", self.api_token)
                .parse()
                .map_err(|e| ApiError::Internal {
                    message: format!("Invalid API token: {}", e),
                })?,
        );
        headers.insert(
            "Content-Type",
            "application/json".parse().map_err(|e| ApiError::Internal {
                message: format!("Invalid content type: {}", e),
            })?,
        );

        if let Some(custom) = custom_headers {
            headers.extend(super::build_headers(custom)?);
        }

        Ok(headers)
    }

    pub(crate) fn build_request(
        &self,
        messages: Vec<Message>,

        stream: bool,
        config: &ApiConfig,
    ) -> QwenRequest {
        let filtered_messages = messages
            .into_iter()
            .filter(|msg| msg.role != Role::System)
            .map(|msg| QwenMessage {
                role: match msg.role {
                    Role::User => Some("user".to_string()),
                    Role::Assistant => Some("assistant".to_string()),
                    Role::System => unreachable!(),
                },
                content: Some(msg.content),
            })
            .collect();

        let default_model = serde_json::json!(DEFAULT_MODEL);
        let model_value = config.body.get("model").unwrap_or(&default_model);

        let default_max_tokens_json = serde_json::json!(8192);
        let mut request_value = serde_json::json!({
            "messages": filtered_messages,
            "stream": stream,
            "model": model_value,
            "max_tokens": config.body.get("max_tokens").unwrap_or(&default_max_tokens_json),
        });

        if let serde_json::Value::Object(mut map) = request_value {
            if let serde_json::Value::Object(mut body) =
                serde_json::to_value(&config.body).unwrap_or_default()
            {
                // Remove protected fields from config body
                body.remove("stream");
                body.remove("messages");
                body.remove("system");

                // Merge remaining fields from config.body
                for (key, value) in body {
                    map.insert(key, value);
                }
            }
            request_value = serde_json::Value::Object(map);
        }

        serde_json::from_value(request_value).unwrap_or_else(|_| QwenRequest {
            messages: filtered_messages,

            stream,
            additional_params: config.body.clone(),
        })
    }

    #[allow(unused)]
    pub async fn chat(&self, messages: Vec<Message>, config: &ApiConfig) -> Result<QwenResponse> {
        let headers = self.build_headers(Some(&config.headers))?;
        let request = self.build_request(messages, false, config);

        let response = self
            .client
            .post(QWEN_API_URL)
            .headers(headers)
            .json(&request)
            .send()
            .await
            .map_err(|e| ApiError::QwenError {
                message: format!("Request failed: {}", e),
                type_: "request_failed".to_string(),
                param: None,
                code: None,
            })?;

        if !response.status().is_success() {
            let error = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(ApiError::QwenError {
                message: error,
                type_: "api_error".to_string(),
                param: None,
                code: None,
            });
        }

        response
            .json::<QwenResponse>()
            .await
            .map_err(|e| ApiError::QwenError {
                message: format!("Failed to parse response: {}", e),
                type_: "parse_error".to_string(),
                param: None,
                code: None,
            })
    }

    pub fn chat_stream(
        &self,
        messages: Vec<Message>,

        config: &ApiConfig,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>> {
        let headers = match self.build_headers(Some(&config.headers)) {
            Ok(h) => h,
            Err(e) => return Box::pin(futures::stream::once(async move { Err(e) })),
        };

        let request = self.build_request(messages, true, config);
        let client = self.client.clone();
        Box::pin(async_stream::try_stream! {
            let mut stream = client
                .post(QWEN_API_URL)
                .headers(headers)
                .json(&request)
                .send()
                .await
                .map_err(|e| ApiError::QwenError {
                    message: format!("Request failed: {}", e),
                    type_: "request_failed".to_string(),
                    param: None,
                    code: None
                })?
                .bytes_stream();

            let mut data = String::new();


            while let Some(chunk) = stream.next().await {

                let chunk = chunk.map_err(|e| ApiError::QwenError {
                    message: format!("Stream error: {}", e),
                    type_: "stream_error".to_string(),
                    param: None,
                    code: None
                })?;
                data.push_str(&String::from_utf8_lossy(&chunk));

                let mut start = 0;
                while let Some(end) = data[start..].find("\n\n") {
                    let end = start + end;
                    let line = &data[start..end];
                    start = end + 2;

                    if line.starts_with("data: ") {
                        let mut json_data=line.to_string();
                        if line.contains("[DONE]") {
                            json_data="{\"data\": \"NONE\"}".to_string();
                        }else{
                            json_data=json_data.replace("data: {", "{\"data\": \"data\",");
                        }
                                if let Ok(event) = serde_json::from_str::<StreamEvent>(json_data.as_str()) {
                                    // info!("event: {:?}", event);
                                    yield event;
                                }
                            }


                }

                if start > 0 {
                    data = data[start..].to_string();
                }
            }
        })
    }
}
