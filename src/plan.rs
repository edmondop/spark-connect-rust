use std::collections::HashMap;

use crate::spark::{self, read::ReadType};

#[derive(Clone)]
pub struct SqlPlan {
    pub(crate) query: String,
}
#[derive(Clone)]
pub struct NamedTableReadPlan {
    pub(crate) table_name: String,
    pub options: HashMap<String, String>,
}

#[derive(Clone)]
pub struct LoadPlan {
    pub paths: Vec<String>,
    pub options: HashMap<String, String>,
    pub format: Option<String>,
    pub schema: Option<String>,
}

pub trait Plan {
    fn collect(&self) -> spark::Relation;

    fn clone(&self) -> Box<dyn Plan>;
}

impl Plan for SqlPlan {
    fn collect(&self) -> spark::Relation {
        spark::Relation {
            common: None,
            rel_type: Some(spark::relation::RelType::Sql(spark::Sql {
                query: self.query.to_string(),
                args: HashMap::new(),
                pos_args: vec![],
            })),
        }
    }

    fn clone(&self) -> Box<dyn Plan> {
        Box::new(SqlPlan {
            query: self.query.clone(),
        })
    }
}

impl Plan for NamedTableReadPlan {
    fn collect(&self) -> spark::Relation {
        let read_relation = spark::Read {
            is_streaming: false,
            read_type: Some(ReadType::NamedTable(spark::read::NamedTable {
                unparsed_identifier: self.table_name.clone(),
                options: self.options.clone(),
            })),
        };
        spark::Relation {
            common: None,
            rel_type: Some(spark::relation::RelType::Read(read_relation)),
        }
    }

    fn clone(&self) -> Box<dyn Plan> {
        Box::new(NamedTableReadPlan {
            table_name: self.table_name.clone(),
            options: self.options.clone(),
        })
    }
}

impl Plan for LoadPlan {
    fn collect(&self) -> spark::Relation {
        let read_relation = spark::Read {
            is_streaming: false,
            read_type: Some(ReadType::DataSource(spark::read::DataSource {
                paths: self.paths.clone(),
                schema: self.schema.clone(),
                options: self.options.clone(),
                format: self.format.clone(),
                predicates: vec![],
            })),
        };
        spark::Relation {
            common: None,
            rel_type: Some(spark::relation::RelType::Read(read_relation)),
        }
    }

    fn clone(&self) -> Box<dyn Plan> {
        Box::new(LoadPlan {
            paths: self.paths.clone(),
            options: self.options.clone(),
            format: self.format.clone(),
            schema: self.schema.clone(),
        })
    }
}

pub struct Project {
    pub expressions: Vec<spark::Expression>,
    pub input: Box<dyn Plan>,
}

impl Plan for Project {
    fn collect(&self) -> spark::Relation {
        spark::Relation {
            common: None,
            rel_type: Some(spark::relation::RelType::Project(Box::new(
                spark::Project {
                    input: Some(Box::new(self.input.collect())),
                    expressions: self.expressions.clone(),
                },
            ))),
        }
    }

    fn clone(&self) -> Box<dyn Plan> {
        return Box::new(Project {
            expressions: self.expressions.clone(),
            input: self.input.clone(),
        });
    }
}
