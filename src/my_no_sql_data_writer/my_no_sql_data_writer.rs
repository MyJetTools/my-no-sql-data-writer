use flurl::{FlUrl, FlUrlResponse};
use my_no_sql_server_abstractions::{DataSyncronizationPeriod, MyNoSqlEntity};
use serde::{de::DeserializeOwned, Serialize};

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

        let reason = response.get_body_as_ut8string().await?;
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

        let reason = response.get_body_as_ut8string().await?;
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

        let reason = response.get_body_as_ut8string().await?;
        return Err(DataWriterError::Error(reason));
    }
}

fn is_ok_result(response: &FlUrlResponse) -> bool {
    response.get_status_code() >= 200 && response.get_status_code() < 300
}

fn serialize_entity_to_body<TEntity: Serialize>(entity: TEntity) -> Option<Vec<u8>> {
    serde_json::to_string(&entity).unwrap().into_bytes().into()
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
            DataSyncronizationPeriod::Min1 => "50",
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
