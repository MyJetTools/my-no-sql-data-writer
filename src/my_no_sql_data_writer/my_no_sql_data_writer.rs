use flurl::{FlUrl, FlUrlResponse};
use my_no_sql_server_abstractions::{DataSyncronizationPeriod, MyNoSqlEntity};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::DataWriterError;

const ROW_CONTROLLER: &str = "Row";

pub struct MyNoSqlDataWriter<TEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + Serialize> {
    url: String,
    table_name: String,
    sync_period: DataSyncronizationPeriod,
    itm: Option<TEntity>,
}

impl<TEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + Serialize>
    MyNoSqlDataWriter<TEntity>
{
    pub fn new(url: String, table_name: String, sync_period: DataSyncronizationPeriod) -> Self {
        Self {
            url,
            table_name,
            itm: None,
            sync_period,
        }
    }

    pub async fn create_table_if_not_exists(&self, persist: bool) -> Result<(), DataWriterError> {
        let response = FlUrl::new(self.url.as_str())
            .append_path_segment("Tables")
            .append_path_segment("CreateIfNotExists")
            .appen_data_sync_period(&self.sync_period)
            .with_persist_as_query_param(persist)
            .with_table_name_as_query_param(self.table_name.as_str())
            .post(None)
            .await?;

        if is_ok_result(&response) {
            return Ok(());
        }

        let reason = response.receive_body().await?;
        let reason = String::from_utf8(reason)?;
        return Err(DataWriterError::Error(reason));
    }

    pub async fn insert_entity(&self, entity: TEntity) -> Result<(), DataWriterError> {
        let response = FlUrl::new(self.url.as_str())
            .append_path_segment(ROW_CONTROLLER)
            .append_path_segment("Insert")
            .appen_data_sync_period(&self.sync_period)
            .with_table_name_as_query_param(self.table_name.as_str())
            .post(serialize_entity_to_body(entity))
            .await?;

        if is_ok_result(&response) {
            return Ok(());
        }

        let reason = response.receive_body().await?;
        let reason = String::from_utf8(reason)?;
        return Err(DataWriterError::Error(reason));
    }

    pub async fn insert_or_replace_entity(&self, entity: TEntity) -> Result<(), DataWriterError> {
        let response = FlUrl::new(self.url.as_str())
            .append_path_segment(ROW_CONTROLLER)
            .append_path_segment("InsertOrReplace")
            .appen_data_sync_period(&self.sync_period)
            .with_table_name_as_query_param(self.table_name.as_str())
            .post(serialize_entity_to_body(entity))
            .await?;

        if is_ok_result(&response) {
            return Ok(());
        }

        let reason = response.receive_body().await?;
        let reason = String::from_utf8(reason)?;
        return Err(DataWriterError::Error(reason));
    }

    pub async fn get_entity(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let mut response = FlUrl::new(self.url.as_str())
            .append_path_segment(ROW_CONTROLLER)
            .with_partition_key_as_query_param(partition_key)
            .with_row_key_as_query_param(row_key)
            .with_table_name_as_query_param(self.table_name.as_str())
            .get()
            .await?;

        if response.get_status_code() == 404 {
            return Ok(None);
        }

        check_error(&mut response).await?;

        if is_ok_result(&response) {
            let entity = deserialize_entity(response.get_body().await?)?;
            return Ok(Some(entity));
        }

        return Ok(None);
    }
}

fn is_ok_result(response: &FlUrlResponse) -> bool {
    response.get_status_code() >= 200 && response.get_status_code() < 300
}

fn deserialize_entity<TEntity: DeserializeOwned>(src: &[u8]) -> Result<TEntity, DataWriterError> {
    let src = std::str::from_utf8(src)?;
    match serde_json::from_str(src) {
        Ok(result) => Ok(result),
        Err(err) => {
            return Err(DataWriterError::Error(format!(
                "Failed to deserialize entity: {:?}",
                err
            )))
        }
    }
}

fn serialize_entity_to_body<TEntity: Serialize>(entity: TEntity) -> Option<Vec<u8>> {
    serde_json::to_string(&entity).unwrap().into_bytes().into()
}

async fn check_error(response: &mut FlUrlResponse) -> Result<(), DataWriterError> {
    match response.get_status_code() {
        400 => {
            return Err(deserialize_error(response).await?);
        }

        409 => {
            return Err(DataWriterError::TableNotFound("".to_string()));
        }
        _ => Ok(()),
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OperationFailHttpContract {
    pub reason: String,
    pub message: String,
}

async fn deserialize_error(
    response: &mut FlUrlResponse,
) -> Result<DataWriterError, DataWriterError> {
    let body = response.get_body().await?;

    let body_as_str = std::str::from_utf8(body)?;

    let result = match serde_json::from_str::<OperationFailHttpContract>(body_as_str) {
        Ok(fail_contract) => match fail_contract.reason.as_str() {
            "TableAlreadyExists" => DataWriterError::TableAlreadyExists(fail_contract.message),
            "TableNotFound" => DataWriterError::TableNotFound(fail_contract.message),
            "RecordAlreadyExists" => DataWriterError::RecordAlreadyExists(fail_contract.message),
            "RequieredEntityFieldIsMissing" => {
                DataWriterError::RequieredEntityFieldIsMissing(fail_contract.message)
            }
            "JsonParseFail" => DataWriterError::ServerCouldNotParseJson(fail_contract.message),
            _ => DataWriterError::Error(format!("Not supported error. {:?}", fail_contract)),
        },
        Err(err) => {
            return Err(DataWriterError::Error(format!(
                "Failed to deserialize error: {:?}",
                err
            )))
        }
    };

    Ok(result)
}

trait FlUrlExt {
    fn with_table_name_as_query_param(self, table_name: &str) -> FlUrl;

    fn appen_data_sync_period(self, sync_period: &DataSyncronizationPeriod) -> FlUrl;

    fn with_partition_key_as_query_param(self, partition_key: &str) -> FlUrl;
    fn with_row_key_as_query_param(self, partition_key: &str) -> FlUrl;

    fn with_persist_as_query_param(self, persist: bool) -> FlUrl;
}

impl FlUrlExt for FlUrl {
    fn with_table_name_as_query_param(self, table_name: &str) -> FlUrl {
        self.append_query_param("tableName", table_name)
    }

    fn appen_data_sync_period(self, sync_period: &DataSyncronizationPeriod) -> FlUrl {
        let value = match sync_period {
            DataSyncronizationPeriod::Immediately => "i",
            DataSyncronizationPeriod::Sec1 => "1",
            DataSyncronizationPeriod::Sec5 => "5",
            DataSyncronizationPeriod::Sec15 => "15",
            DataSyncronizationPeriod::Sec30 => "30",
            DataSyncronizationPeriod::Min1 => "60",
            DataSyncronizationPeriod::Asap => "a",
        };

        self.append_query_param("syncPeriod", value)
    }

    fn with_partition_key_as_query_param(self, partition_key: &str) -> FlUrl {
        self.append_query_param("partitionKey", partition_key)
    }

    fn with_row_key_as_query_param(self, row_key: &str) -> FlUrl {
        self.append_query_param("rowKey", row_key)
    }
    fn with_persist_as_query_param(self, persist: bool) -> FlUrl {
        let value = if persist { "1" } else { "0" };
        self.append_query_param("persist", value)
    }
}
