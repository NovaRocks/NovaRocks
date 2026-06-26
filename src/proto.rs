pub mod novarocks {
    tonic::include_proto!("novarocks");
}

pub mod starrocks {
    #![allow(clippy::doc_lazy_continuation, clippy::len_without_is_empty)]
    tonic::include_proto!("starrocks");
}

pub mod staros {
    tonic::include_proto!("staros");
}
