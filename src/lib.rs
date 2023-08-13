pub mod spark {
    tonic::include_proto!("spark.connect");
}

mod arrow;
pub mod dataframe;
pub mod error;
mod plan;
mod session;

pub use session::RemoteSparkSession;
