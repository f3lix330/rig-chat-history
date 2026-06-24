use anyhow::Result;
use log::error;
use redb::{
    AccessGuard, Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, Table,
    TableDefinition,
};
use rig_core::completion::Message;
use rig_core::memory::{ConversationMemory, MemoryError, MessageFilter};
use rig_core::wasm_compat::WasmBoxedFuture;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub enum CacheLoad<T: AsRef<str>> {
    NoCache,
    NewCache,
    TryExistingCache(T),
}

pub struct REDBMemory {
    cache: Option<Arc<Mutex<Vec<Message>>>>,
    database: Arc<Mutex<Database>>,
    filter: Option<Arc<dyn MessageFilter>>,
}

impl REDBMemory {
    pub fn new<T, S>(path: T, cache: CacheLoad<S>) -> Result<Self>
    where
        T: AsRef<Path>,
        S: AsRef<str>,
    {
        let database = Database::create(path)?;

        let history = match cache {
            CacheLoad::NoCache => None,
            CacheLoad::NewCache => Some(Arc::new(Mutex::new(Vec::new()))),
            CacheLoad::TryExistingCache(table) => {
                let transaction = database.begin_write()?;
                let table =
                    transaction.open_table(TableDefinition::<u64, Vec<u8>>::new(table.as_ref()))?;
                match table
                    .range(0..table.len().unwrap_or_default())?
                    .map(|item| {
                        let (_, value) = item.map_err(|e| MemoryError::Internal(e.to_string()))?;
                        rmp_serde::from_slice::<Message>(&value.value())
                            .map_err(|e| MemoryError::Internal(e.to_string()))
                    })
                    .collect::<Result<Vec<Message>, MemoryError>>()
                {
                    Ok(cache) => Some(Arc::new(Mutex::new(cache))),
                    Err(e) => {
                        error!("Failed to load from cache: {e}");
                        None
                    }
                }
            }
        };

        let memory = REDBMemory {
            cache: history,
            database: Arc::new(Mutex::new(database)),
            filter: None,
        };
        Ok(memory)
    }

    pub fn with_filter<F>(mut self, filter: F) -> Self
    where
        F: MessageFilter + 'static,
    {
        self.filter = Some(Arc::new(filter));
        self
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Database>, MemoryError> {
        self.database
            .lock()
            .map_err(|e| MemoryError::Internal(e.to_string()))
    }

    fn get_all_messages(&self, conversation_id: &str) -> Result<Vec<Vec<u8>>, MemoryError> {
        let guard = self.lock()?;

        let txn = guard
            .begin_read()
            .map_err(|e| MemoryError::Internal(e.to_string()))?;

        let table = txn
            .open_table(TableDefinition::new(conversation_id))
            .map_err(|e| MemoryError::Internal(e.to_string()))?;

        let mut messages = vec![];

        for message in table
            .range(0..table.len().unwrap_or_default())
            .map_err(|e| MemoryError::Internal(e.to_string()))?
        {
            let (_, value): (AccessGuard<u64>, AccessGuard<Vec<u8>>) =
                message.map_err(|e| MemoryError::Internal(e.to_string()))?;

            messages.push(value.value());
        }
        Ok(messages)
    }
}

impl ConversationMemory for REDBMemory {
    fn load<'a>(
        &'a self,
        conversation_id: &'a str,
    ) -> WasmBoxedFuture<'a, Result<Vec<Message>, MemoryError>> {
        Box::pin(async move {
            let messages = {
                let mut messages = vec![];

                if let Some(cache) = &self.cache {
                    let cache = cache
                        .lock()
                        .map_err(|e| MemoryError::Internal(e.to_string()))?;

                    messages.extend_from_slice(&cache);
                    messages
                } else {
                    for message in self.get_all_messages(conversation_id).unwrap_or_default() {
                        let message = rmp_serde::from_slice(&message)
                            .map_err(|e| MemoryError::Internal(e.to_string()))?;
                        messages.push(message);
                    }
                    messages
                }
            };
            match &self.filter {
                Some(filter) => Ok(filter(messages)),
                None => Ok(messages),
            }
        })
    }

    fn append<'a>(
        &'a self,
        conversation_id: &'a str,
        mut messages: Vec<Message>,
    ) -> WasmBoxedFuture<'a, Result<(), MemoryError>> {
        Box::pin(async move {
            let guard = self.lock()?;

            let txn = guard
                .begin_write()
                .map_err(|e| MemoryError::Internal(e.to_string()))?;

            {
                let mut table: Table<u64, Vec<u8>> = txn
                    .open_table(TableDefinition::new(conversation_id))
                    .map_err(|e| MemoryError::Internal(e.to_string()))?;

                for message in &messages {
                    let last_index = table.len().unwrap_or_default();
                    table
                        .insert(last_index, rmp_serde::to_vec(&message).unwrap_or_default())
                        .map_err(|e| MemoryError::Internal(e.to_string()))?;
                }

                if let Some(cache) = &self.cache {
                    let mut cache = cache
                        .lock()
                        .map_err(|e| MemoryError::Internal(e.to_string()))?;
                    cache.append(&mut messages);
                }
            }
            txn.commit()
                .map_err(|e| MemoryError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn clear<'a>(
        &'a self,
        conversation_id: &'a str,
    ) -> WasmBoxedFuture<'a, Result<(), MemoryError>> {
        Box::pin(async move {
            let guard = self.lock()?;

            if let Some(cache) = &self.cache {
                let mut cache = cache
                    .lock()
                    .map_err(|e| MemoryError::Internal(e.to_string()))?;
                cache.clear();
            }

            let txn = guard
                .begin_write()
                .map_err(|e| MemoryError::Internal(e.to_string()))?;

            txn.delete_table(TableDefinition::<u64, String>::new(conversation_id))
                .map_err(|e| MemoryError::Internal(e.to_string()))?;

            txn.commit()
                .map_err(|e| MemoryError::Internal(e.to_string()))?;

            Ok(())
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::CacheLoad::*;
    use nanoid::nanoid;
    use rig_core::message::Message::User;
    use rig_core::message::UserContent::Text;
    use rig_core::schemars::_private::serde_json::Value;
    use rig_core::schemars::_private::serde_json::Value::Object;
    use rig_core::OneOrMany;
    use std::fs;

    static CONVERSATION_ID: &str = "1";

    #[tokio::test]
    async fn test_append_message() {
        let database_name = nanoid!();
        let memory = REDBMemory::new(&database_name, NoCache::<&str>).unwrap();
        let message = Message::system("Test Message");
        let _ = memory.append(CONVERSATION_ID, vec![message]).await.unwrap();
        let messages = memory.load(CONVERSATION_ID).await.unwrap();
        fs::remove_file(database_name).unwrap();
        assert_eq!(messages, vec![Message::system("Test Message")]);
    }

    #[tokio::test]
    async fn test_append_message_correct_index() {
        let database_name = nanoid!();
        let memory = REDBMemory::new(&database_name, NoCache::<&str>).unwrap();
        let message1 = Message::system("Test Message");
        let message2 = Message::system("Test Message");
        let message3 = Message::system("Test Message 3");
        let _ = memory
            .append(CONVERSATION_ID, vec![message1, message2, message3])
            .await
            .unwrap();
        let messages = memory.load(CONVERSATION_ID).await.unwrap();
        fs::remove_file(database_name).unwrap();
        assert_eq!(*messages.get(2).unwrap(), Message::system("Test Message 3"));
    }

    #[tokio::test]
    async fn test_no_message() {
        let database_name = nanoid!();
        let memory = REDBMemory::new(&database_name, NoCache::<&str>).unwrap();
        let messages = memory.load(CONVERSATION_ID).await.unwrap();
        fs::remove_file(database_name).unwrap();
        assert_eq!(messages, vec![]);
    }

    #[tokio::test]
    async fn test_drop() {
        let database_name = nanoid!();
        let memory = REDBMemory::new(&database_name, NoCache::<&str>).unwrap();
        let message = Message::system("Test Message");

        memory.append(CONVERSATION_ID, vec![message]).await.unwrap();

        memory.clear(CONVERSATION_ID).await.unwrap();

        let messages = memory.load(CONVERSATION_ID).await.unwrap();
        fs::remove_file(database_name).unwrap();
        assert_eq!(Vec::<Message>::new(), messages)
    }

    #[tokio::test]
    async fn test_append_message_cache() {
        let database_name = nanoid!();
        let memory = REDBMemory::new(&database_name, NewCache::<&str>).unwrap();
        let message = Message::system("Test Message");
        let _ = memory.append(CONVERSATION_ID, vec![message]).await.unwrap();
        let messages = memory.load(CONVERSATION_ID).await.unwrap();

        let table = memory
            .database
            .lock()
            .unwrap()
            .begin_read()
            .unwrap()
            .open_table(TableDefinition::<u64, Vec<u8>>::new(CONVERSATION_ID))
            .unwrap();
        let messages_in_db = table
            .range(0..table.len().unwrap())
            .unwrap()
            .map(|m| m.unwrap().1)
            .collect::<Vec<_>>()
            .iter()
            .map(|s| rmp_serde::from_slice::<Message>((&s.value()).as_ref()).unwrap())
            .collect::<Vec<_>>();

        fs::remove_file(database_name).unwrap();

        assert_eq!(messages, vec![Message::system("Test Message")]);
        assert_eq!(messages_in_db, vec![Message::system("Test Message")]);
    }

    #[tokio::test]
    async fn test_append_message_correct_index_cache() {
        let database_name = nanoid!();
        let memory = REDBMemory::new(&database_name, NewCache::<&str>).unwrap();
        let message1 = Message::system("Test Message");
        let message2 = Message::system("Test Message");
        let message3 = Message::system("Test Message 3");
        let _ = memory
            .append(CONVERSATION_ID, vec![message1, message2, message3])
            .await
            .unwrap();
        let messages = memory.load(CONVERSATION_ID).await.unwrap();

        let table = memory
            .database
            .lock()
            .unwrap()
            .begin_read()
            .unwrap()
            .open_table(TableDefinition::<u64, Vec<u8>>::new(CONVERSATION_ID))
            .unwrap();
        let messages_in_db = table
            .range(0..table.len().unwrap())
            .unwrap()
            .map(|m| m.unwrap().1)
            .collect::<Vec<_>>()
            .iter()
            .map(|s| rmp_serde::from_slice::<Message>((&s.value()).as_ref()).unwrap())
            .collect::<Vec<_>>();

        fs::remove_file(database_name).unwrap();
        assert_eq!(*messages.get(2).unwrap(), Message::system("Test Message 3"));
        assert_eq!(
            *messages_in_db.get(2).unwrap(),
            Message::system("Test Message 3")
        );
    }

    #[tokio::test]
    async fn test_no_message_cache() {
        let database_name = nanoid!();
        let memory = REDBMemory::new(&database_name, NewCache::<&str>).unwrap();
        let messages = memory.load(CONVERSATION_ID).await.unwrap();

        fs::remove_file(database_name).unwrap();

        assert_eq!(messages, vec![]);
    }

    #[tokio::test]
    async fn test_drop_cache() {
        let database_name = nanoid!();
        let memory = REDBMemory::new(&database_name, NewCache::<&str>).unwrap();
        let message = Message::system("Test Message");

        memory.append(CONVERSATION_ID, vec![message]).await.unwrap();

        memory.clear(CONVERSATION_ID).await.unwrap();

        let messages = memory.load(CONVERSATION_ID).await.unwrap();

        fs::remove_file(database_name).unwrap();

        assert_eq!(Vec::<Message>::new(), messages);
    }

    #[tokio::test]
    async fn test_load_chat_into_cache() {
        let memory = REDBMemory::new("database/chat", TryExistingCache("12345")).unwrap();

        let messages = memory.load("12345").await.unwrap();

        assert_eq!(
            &User {
                content: OneOrMany::one(Text(rig_core::agent::Text {
                    text: "Ich bin 20 Jahre alt\r\n".to_string(),
                    additional_params: Some(Object(serde_json::map::Map::<String, Value>::new())),
                }))
            },
            messages.get(0).unwrap()
        );
    }

    #[tokio::test]
    async fn test_load_chat_into_cache_table_missing() {
        let memory =
            REDBMemory::new("database/chat_duplicate", TryExistingCache("123456")).unwrap();

        let messages = memory.load("123456").await.unwrap();

        assert_eq!(Vec::<Message>::new(), messages);
    }
}
