use tonic;
pub fn add(left: usize, right: usize) -> usize {
    left + right
}
pub mod spark {
    //tonic::include_proto!("spark.connect");
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
