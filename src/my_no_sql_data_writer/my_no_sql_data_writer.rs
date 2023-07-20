use std::sync::Arc;

use flurl::{FlUrl, FlUrlResponse};
use my_logger::LogEventCtx;
use my_no_sql_server_abstractions::{DataSynchronizationPeriod, MyNoSqlEntity};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::MyNoSqlWriterSettings;

use super::{DataWriterError, UpdateReadStatistics};

const ROW_CONTROLLER: &str = "Row";
const ROWS_CONTROLLER: &str = "Rows";
const BULK_CONTROLLER: &str = "Bulk";

pub struct CreateTableParams {
    pub persist: bool,
    pub max_partitions_amount: Option<usize>,
    pub max_rows_per_partition_amount: Option<usize>,
}

impl CreateTableParams {
    pub fn populate_params(&self, mut fl_url: FlUrl) -> FlUrl {
        if let Some(max_partitions_amount) = self.max_partitions_amount {
            fl_url = fl_url.append_query_param(
                "maxPartitionsAmount",
                Some(max_partitions_amount.to_string()),
            )
        };

        if let Some(max_rows_per_partition_amount) = self.max_rows_per_partition_amount {
            fl_url = fl_url.append_query_param(
                "maxRowsPerPartitionAmount",
                Some(max_rows_per_partition_amount.to_string()),
            )
        };

        if !self.persist {
            fl_url = fl_url.append_query_param("persist", Some("false"));
        };

        fl_url
    }
}

pub struct MyNoSqlDataWriter<TEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + Serialize> {
    settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
    sync_period: DataSynchronizationPeriod,
    itm: Option<TEntity>,
}

impl<TEntity: MyNoSqlEntity + Sync + Send + DeserializeOwned + Serialize>
    MyNoSqlDataWriter<TEntity>
{
    //To Remove warning of itm
    pub fn do_not_use_it(&self) -> &Option<TEntity> {
        &self.itm
    }

    pub fn new(
        settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
        auto_create_table_params: Option<CreateTableParams>,
        sync_period: DataSynchronizationPeriod,
    ) -> Self {
        if let Some(create_table_params) = auto_create_table_params {
            tokio::spawn(create_table_if_not_exists(
                settings.clone(),
                TEntity::TABLE_NAME,
                create_table_params,
                sync_period,
            ));
        }

        Self {
            settings,
            itm: None,
            sync_period,
        }
    }

    async fn get_fl_url(&self) -> FlUrl {
        let url = self.settings.get_url().await;
        FlUrl::new(url)
    }

    pub async fn create_table(&self, params: CreateTableParams) -> Result<(), DataWriterError> {
        let url = self.settings.get_url().await;
        let fl_url = FlUrl::new(url.as_str());

        let fl_url = fl_url
            .append_path_segment("Tables")
            .append_path_segment("Create")
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .append_data_sync_period(&self.sync_period);

        let fl_url = params.populate_params(fl_url);

        let mut response = fl_url.post(None).await?;

        create_table_errors_handler(&mut response, "create_table", url.as_str()).await
    }

    pub async fn create_table_if_not_exists(
        &self,
        params: CreateTableParams,
    ) -> Result<(), DataWriterError> {
        create_table_if_not_exists(
            self.settings.clone(),
            TEntity::TABLE_NAME,
            params,
            self.sync_period,
        )
        .await
    }

    pub async fn insert_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let response = self
            .get_fl_url()
            .await
            .append_path_segment(ROW_CONTROLLER)
            .append_path_segment("Insert")
            .append_data_sync_period(&self.sync_period)
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .post(serialize_entity_to_body(entity))
            .await?;

        if is_ok_result(&response) {
            return Ok(());
        }

        let reason = response.receive_body().await?;
        let reason = String::from_utf8(reason)?;
        return Err(DataWriterError::Error(reason));
    }

    pub async fn insert_or_replace_entity(&self, entity: &TEntity) -> Result<(), DataWriterError> {
        let response = self
            .get_fl_url()
            .await
            .append_path_segment(ROW_CONTROLLER)
            .append_path_segment("InsertOrReplace")
            .append_data_sync_period(&self.sync_period)
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .post(serialize_entity_to_body(entity))
            .await?;

        if is_ok_result(&response) {
            return Ok(());
        }

        let reason = response.receive_body().await?;
        let reason = String::from_utf8(reason)?;
        return Err(DataWriterError::Error(reason));
    }

    pub async fn bulk_insert_or_replace(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let response = self
            .get_fl_url()
            .await
            .append_path_segment(BULK_CONTROLLER)
            .append_path_segment("InsertOrReplace")
            .append_data_sync_period(&self.sync_period)
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .post(serialize_entities_to_body(entities))
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
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let mut request = self
            .get_fl_url()
            .await
            .append_path_segment(ROW_CONTROLLER)
            .with_partition_key_as_query_param(partition_key)
            .with_row_key_as_query_param(row_key)
            .with_table_name_as_query_param(TEntity::TABLE_NAME);

        if let Some(update_read_statistics) = update_read_statistics {
            request = update_read_statistics.fill_fields(request);
        }

        let mut response = request.get().await?;

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

    pub async fn get_by_partition_key(
        &self,
        partition_key: &str,
        update_read_statistics: Option<UpdateReadStatistics>,
    ) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let mut request = self
            .get_fl_url()
            .await
            .append_path_segment(ROW_CONTROLLER)
            .with_partition_key_as_query_param(partition_key)
            .with_table_name_as_query_param(TEntity::TABLE_NAME);

        if let Some(update_read_statistics) = update_read_statistics {
            request = update_read_statistics.fill_fields(request);
        }

        let mut response = request.get().await?;

        if response.get_status_code() == 404 {
            return Ok(None);
        }

        check_error(&mut response).await?;

        if is_ok_result(&response) {
            let entities = deserialize_entities(response.get_body().await?)?;
            return Ok(Some(entities));
        }

        return Ok(None);
    }

    pub async fn get_by_row_key(
        &self,
        row_key: &str,
    ) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let mut response = self
            .get_fl_url()
            .await
            .append_path_segment(ROW_CONTROLLER)
            .with_row_key_as_query_param(row_key)
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .get()
            .await?;

        if response.get_status_code() == 404 {
            return Ok(None);
        }

        check_error(&mut response).await?;

        if is_ok_result(&response) {
            let entities = deserialize_entities(response.get_body().await?)?;
            return Ok(Some(entities));
        }

        return Ok(None);
    }

    pub async fn delete_row(
        &self,
        partition_key: &str,
        row_key: &str,
    ) -> Result<Option<TEntity>, DataWriterError> {
        let mut response = self
            .get_fl_url()
            .await
            .append_path_segment(ROW_CONTROLLER)
            .with_partition_key_as_query_param(partition_key)
            .with_row_key_as_query_param(row_key)
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .delete()
            .await?;

        if response.get_status_code() == 404 {
            return Ok(None);
        }

        check_error(&mut response).await?;

        if response.get_status_code() == 200 {
            let entity = deserialize_entity(response.get_body().await?)?;
            return Ok(Some(entity));
        }

        return Ok(None);
    }

    pub async fn delete_partitions(&self, partition_keys: &[&str]) -> Result<(), DataWriterError> {
        let mut response = self
            .get_fl_url()
            .await
            .append_path_segment(ROWS_CONTROLLER)
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .with_partition_keys_as_query_param(partition_keys)
            .delete()
            .await?;

        if response.get_status_code() == 404 {
            return Ok(());
        }

        check_error(&mut response).await?;

        return Ok(());
    }

    pub async fn get_all(&self) -> Result<Option<Vec<TEntity>>, DataWriterError> {
        let mut response = self
            .get_fl_url()
            .await
            .append_path_segment(ROW_CONTROLLER)
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .get()
            .await?;

        if response.get_status_code() == 404 {
            return Ok(None);
        }

        check_error(&mut response).await?;

        if is_ok_result(&response) {
            let entities = deserialize_entities(response.get_body().await?)?;
            return Ok(Some(entities));
        }

        return Ok(None);
    }

    pub async fn clean_table_and_bulk_insert(
        &self,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let mut response = self
            .get_fl_url()
            .await
            .append_path_segment(BULK_CONTROLLER)
            .append_path_segment("CleanAndBulkInsert")
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .append_data_sync_period(&self.sync_period)
            .post(serialize_entities_to_body(entities))
            .await?;

        check_error(&mut response).await?;

        return Ok(());
    }

    pub async fn clean_partition_and_bulk_insert(
        &self,
        partition_key: &str,
        entities: &[TEntity],
    ) -> Result<(), DataWriterError> {
        let mut response = self
            .get_fl_url()
            .await
            .append_path_segment(BULK_CONTROLLER)
            .append_path_segment("CleanAndBulkInsert")
            .with_table_name_as_query_param(TEntity::TABLE_NAME)
            .append_data_sync_period(&self.sync_period)
            .with_partition_key_as_query_param(partition_key)
            .post(serialize_entities_to_body(entities))
            .await?;

        check_error(&mut response).await?;

        return Ok(());
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

fn deserialize_entities<TEntity: DeserializeOwned>(
    src: &[u8],
) -> Result<Vec<TEntity>, DataWriterError> {
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

fn serialize_entity_to_body<TEntity: Serialize>(entity: &TEntity) -> Option<Vec<u8>> {
    serde_json::to_string(&entity).unwrap().into_bytes().into()
}

fn serialize_entities_to_body<TEntity: Serialize>(entities: &[TEntity]) -> Option<Vec<u8>> {
    serde_json::to_string(&entities)
        .unwrap()
        .into_bytes()
        .into()
}

async fn check_error(response: &mut FlUrlResponse) -> Result<(), DataWriterError> {
    let result = match response.get_status_code() {
        400 => Err(deserialize_error(response).await?),

        409 => Err(DataWriterError::TableNotFound("".to_string())),
        _ => Ok(()),
    };

    if let Err(err) = &result {
        my_logger::LOGGER.write_error(
            format!("FlUrlRequest to {}", response.url.to_string()),
            format!("{:?}", err),
            None.into(),
        );
    }

    result
}

async fn create_table_errors_handler(
    response: &mut FlUrlResponse,
    process_name: &'static str,
    url: &str,
) -> Result<(), DataWriterError> {
    if is_ok_result(response) {
        return Ok(());
    }

    let result = deserialize_error(response).await?;

    my_logger::LOGGER.write_error(
        process_name,
        format!("{:?}", result),
        LogEventCtx::new().add("URL", url),
    );

    Err(result)
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
            "RequiredEntityFieldIsMissing" => {
                DataWriterError::RequiredEntityFieldIsMissing(fail_contract.message)
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

    fn append_data_sync_period(self, sync_period: &DataSynchronizationPeriod) -> FlUrl;

    fn with_partition_key_as_query_param(self, partition_key: &str) -> FlUrl;
    fn with_partition_keys_as_query_param(self, partition_keys: &[&str]) -> FlUrl;
    fn with_row_key_as_query_param(self, partition_key: &str) -> FlUrl;

    fn with_persist_as_query_param(self, persist: bool) -> FlUrl;
}

impl FlUrlExt for FlUrl {
    fn with_table_name_as_query_param(self, table_name: &str) -> FlUrl {
        self.append_query_param("tableName", Some(table_name))
    }

    fn append_data_sync_period(self, sync_period: &DataSynchronizationPeriod) -> FlUrl {
        let value = match sync_period {
            DataSynchronizationPeriod::Immediately => "i",
            DataSynchronizationPeriod::Sec1 => "1",
            DataSynchronizationPeriod::Sec5 => "5",
            DataSynchronizationPeriod::Sec15 => "15",
            DataSynchronizationPeriod::Sec30 => "30",
            DataSynchronizationPeriod::Min1 => "60",
            DataSynchronizationPeriod::Asap => "a",
        };

        self.append_query_param("syncPeriod", Some(value))
    }

    fn with_partition_key_as_query_param(self, partition_key: &str) -> FlUrl {
        self.append_query_param("partitionKey", Some(partition_key))
    }

    fn with_partition_keys_as_query_param(self, partition_keys: &[&str]) -> FlUrl {
        let mut s = self;
        for partition_key in partition_keys {
            s = s.append_query_param("partitionKey", Some(*partition_key));
        }
        s
    }

    fn with_row_key_as_query_param(self, row_key: &str) -> FlUrl {
        self.append_query_param("rowKey", Some(row_key))
    }
    fn with_persist_as_query_param(self, persist: bool) -> FlUrl {
        let value = if persist { "1" } else { "0" };
        self.append_query_param("persist", Some(value))
    }
}

async fn create_table_if_not_exists(
    settings: Arc<dyn MyNoSqlWriterSettings + Send + Sync + 'static>,
    table_name: &'static str,
    params: CreateTableParams,
    sync_period: DataSynchronizationPeriod,
) -> Result<(), DataWriterError> {
    let url = settings.get_url().await;
    let fl_url = FlUrl::new(url.as_str())
        .append_path_segment("Tables")
        .append_path_segment("CreateIfNotExists")
        .append_data_sync_period(&sync_period)
        .with_table_name_as_query_param(table_name);

    let fl_url = params.populate_params(fl_url);

    let mut response = fl_url.post(None).await?;

    create_table_errors_handler(&mut response, "create_table_if_not_exists", url.as_str()).await
}

#[cfg(test)]
mod tests {
    use my_no_sql_server_abstractions::MyNoSqlEntity;
    use serde::Serialize;

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "PascalCase")]
    struct TestEntity {
        partition_key: String,
        row_key: String,
    }

    impl MyNoSqlEntity for TestEntity {
        const TABLE_NAME: &'static str = "test";

        fn get_partition_key(&self) -> &str {
            &self.partition_key
        }

        fn get_row_key(&self) -> &str {
            &self.row_key
        }

        fn get_time_stamp(&self) -> i64 {
            0
        }
    }

    #[test]
    fn test() {
        let entities = vec![
            TestEntity {
                partition_key: "1".to_string(),
                row_key: "1".to_string(),
            },
            TestEntity {
                partition_key: "1".to_string(),
                row_key: "2".to_string(),
            },
            TestEntity {
                partition_key: "2".to_string(),
                row_key: "1".to_string(),
            },
            TestEntity {
                partition_key: "2".to_string(),
                row_key: "2".to_string(),
            },
        ];

        let as_json = super::serialize_entities_to_body(&entities).unwrap();

        println!("{}", std::str::from_utf8(&as_json).unwrap());
    }
}
