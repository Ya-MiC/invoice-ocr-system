use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::fs;
use std::path::Path;
use sha2::{Sha256, Digest};

/// 计算文件SHA256哈希（用于去重）
#[pyfunction]
fn compute_file_hash(py: Python, file_path: &str) -> PyResult<String> {
    let data = fs::read(file_path)?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let hash = format!("{:x}", hasher.finalize());
    Ok(hash)
}

/// 检查文件是否存在
#[pyfunction]
fn file_exists(file_path: &str) -> bool {
    Path::new(file_path).exists()
}

/// 获取文件大小
#[pyfunction]
fn get_file_size(file_path: &str) -> PyResult<u64> {
    let metadata = fs::metadata(file_path)?;
    Ok(metadata.len())
}

/// 创建目录
#[pyfunction]
fn create_dir(dir_path: &str) -> PyResult<bool> {
    fs::create_dir_all(dir_path)?;
    Ok(true)
}

/// 移动文件
#[pyfunction]
fn move_file(src: &str, dst: &str) -> PyResult<bool> {
    fs::rename(src, dst)?;
    Ok(true)
}

/// 获取目录下所有图片文件
#[pyfunction]
fn list_image_files(py: Python, dir_path: &str) -> PyResult<Vec<String>> {
    let extensions = vec!["jpg", "jpeg", "png", "bmp", "pdf"];
    let mut files = Vec::new();
    
    if let Ok(entries) = fs::read_dir(dir_path) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if extensions.contains(&ext.to_string_lossy().to_lowercase().as_str()) {
                        files.push(path.to_string_lossy().to_string());
                    }
                }
            }
        }
    }
    
    Ok(files)
}

/// PyO3模块定义
#[pymodule]
fn invoice_ocr_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compute_file_hash, m)?)?;
    m.add_function(wrap_pyfunction!(file_exists, m)?)?;
    m.add_function(wrap_pyfunction!(get_file_size, m)?)?;
    m.add_function(wrap_pyfunction!(create_dir, m)?)?;
    m.add_function(wrap_pyfunction!(move_file, m)?)?;
    m.add_function(wrap_pyfunction!(list_image_files, m)?)?;
    Ok(())
}
