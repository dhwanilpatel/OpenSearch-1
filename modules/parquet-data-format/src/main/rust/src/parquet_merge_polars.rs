use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use std::error::Error;
use std::path::Path;
use polars::prelude::*;
use polars::lazy::dsl::col;
use polars::prelude::Expr::Selector;
use polars::prelude::Selector::ByName;

const ROW_ID_COLUMN_NAME: &str = "___row_id";
const SORT_COLUMN_NAME: &str = "EventDate";

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesInRust(
    mut env: JNIEnv,
    _class: JClass,
    input_files: JObject,
    output_file: JString,
) -> jint {
    // Add debug logging and validation
    if input_files.is_null() {
        let _ = env.throw_new("java/lang/RuntimeException", "Input files list is null");
        return -1;
    }

    if output_file.is_null() {
        let _ = env.throw_new("java/lang/RuntimeException", "Output file path is null");
        return -1;
    }

    // Extract JNI operations outside catch_unwind
    let input_files_vec = match convert_java_list_to_vec(&mut env, input_files) {
        Ok(vec) => {
            if vec.is_empty() {
                let _ = env.throw_new("java/lang/RuntimeException", "No input files provided");
                return -1;
            }
            vec
        },
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("Failed to convert input files: {:?}", e));
            return -1;
        }
    };

    let output_path: String = match env.get_string(&output_file) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("Failed to get output path: {:?}", e));
            return -1;
        }
    };

    match merge_parquet_files_sorted(&input_files_vec, &output_path) {
        Ok(_) => 0,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("{:?}", e));
            -1
        }
    }
}

fn merge_parquet_files_sorted(input_files: &[String], output_path: &str) -> PolarsResult<()> {
    if input_files.is_empty() {
        return Err(PolarsError::InvalidOperation("No input files".into()));
    }

    println!("Starting merge_sorted approach with {} files", input_files.len());

    // Check if output directory exists
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)
                .map_err(|e| PolarsError::InvalidOperation(format!("Failed to create directory: {}", e).into()))?;
        }
    }

    // Start with first file
    let mut result_lf = LazyFrame::scan_parquet(
        PlPath::Local(Arc::from(Path::new(&input_files[0]))),
        ScanArgsParquet::default()
    )?
    .drop(ByName {names: Arc::new([PlSmallStr::from_str(ROW_ID_COLUMN_NAME)]), strict: true});// Drop existing row_id


    // Merge remaining files using merge_sorted
    for file_path in &input_files[1..] {
        println!("Merging file: {}", file_path);
        let other_lf = LazyFrame::scan_parquet(
            PlPath::Local(Arc::from(Path::new(file_path))),
            ScanArgsParquet::default()
        )?
        .drop(ByName {names: Arc::new([PlSmallStr::from_str(ROW_ID_COLUMN_NAME)]), strict: true});// Drop existing row_id

        result_lf = result_lf.merge_sorted(other_lf, SORT_COLUMN_NAME)?;
    }

    // Add row index and write using streaming
    println!("Writing merged result to: {}", output_path);

    let res = result_lf
        .with_row_index(ROW_ID_COLUMN_NAME, None)  // Add sequential 0-n row_id
        .sink_parquet(
            SinkTarget::Path(PlPath::Local(Arc::from(Path::new(output_path)))),
            ParquetWriteOptions::default(),
            None,
            SinkOptions::default()
        )?;

    let finalRes = res.collect_with_engine(Engine::Streaming);
    //
    // println!("final count === {}", finalRes?.height());

    // if std::path::Path::new(output_path).exists() {
    //     println!("Merge completed successfully");
    // } else {
    //     println!("WARNING: Operation completed but file not found");
    // }

    Ok(())
}

fn convert_java_list_to_vec(env: &mut JNIEnv, list: JObject) -> Result<Vec<String>, Box<dyn Error>> {
    if list.is_null() {
        return Err("Input list is null".into());
    }

    let list_size = env.call_method(&list, "size", "()I", &[])?.i()? as usize;
    let mut result = Vec::with_capacity(list_size);

    for i in 0..list_size {
        let item = env.call_method(&list, "get", "(I)Ljava/lang/Object;", &[(i as i32).into()])?.l()?;
        if item.is_null() {
            continue;
        }
        let jstring = env.call_method(&item, "toString", "()Ljava/lang/String;", &[])?.l()?;
        let string_item = env.get_string(&jstring.into())?.into();
        result.push(string_item);
    }

    Ok(result)
}
