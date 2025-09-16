// organizar_xml_cte_nfe_dest.rs
// Versão em Rust do script para organizar arquivos .xml e .zip conforme as regras do usuário.
// Opção --dest agora disponível (padrão: ~/docs_fiscais) — todas as pastas NFe/CTe/MDFe/NFCe serão criadas dentro de ~/docs_fiscais.
//
// Instruções rápidas:
// 1) Crie um novo projeto cargo:
//      cargo new organizar_xml
// 2) Substitua src/main.rs pelo conteúdo deste arquivo.
// 3) Adicione as dependências no Cargo.toml (exemplo abaixo).
// 4) Build & run:
//      cargo run --release -- --root /home/usuario --dest ~/docs_fiscais --dry-run -v
//
// Exemplo (adicionar ao Cargo.toml):
// [dependencies]
// clap = { version = "4", features = ["derive"] }
// walkdir = "2"
// regex = "1"
// zip = "0.6"
// chrono = { version = "0.4", features = ["serde"] }
// rayon = "1.7"
// tempfile = "3"
// dirs = "4"
//
// Comportamento:
// - Percorre recursivamente --root (padrão: $HOME)
// - Se encontrar .zip, extrai em um tempdir com proteção contra path traversal
// - Procura em nomes de arquivos um bloco de 44 dígitos (primeiro encontrado)
// - model = chave[20..22], mapeado para 55->NFe, 57->CTe, 58->MDFe, 65->NFCe
// - Para 55 e 57, interpreta ano (chave[2..4]) e mês (chave[4..6]) e decide "Mais de 6 meses" vs "Menos de 6 meses"
// - Copia arquivos (não move), criando diretórios e evitando sobrescrita adicionando sufixos _1, _2...

use clap::Parser;
use chrono::{Datelike, Local};
use regex::Regex;
use std::path::{Path, PathBuf};
use std::fs;
use walkdir::WalkDir;
use zip::read::ZipArchive;
use std::fs::File;
use tempfile::tempdir;
use rayon::prelude::*;
use std::io::{self};

#[derive(Parser, Debug)]
#[command(author, version, about = "Organiza XML (NFe/CTe/MDFe/NFCe) por chave de acesso", long_about = None)]
struct Args {
    /// Diretório raiz para procurar (padrão: $HOME)
    #[arg(short, long, value_parser(clap::value_parser!(PathBuf)))]
    root: Option<PathBuf>,

    /// Diretório onde serão criadas as pastas NFe/CTe/MDFe/NFCe (padrão: ~/docs_fiscais)
    #[arg(long, value_parser(clap::value_parser!(PathBuf)))]
    dest: Option<PathBuf>,

    /// Simula as operações sem copiar
    #[arg(long)]
    dry_run: bool,

    /// Número de workers para cópias (paralelismo)
    #[arg(short, long, default_value_t = 4)]
    workers: usize,

    /// Verbose
    #[arg(short, long)]
    verbose: bool,
}

const CHAVE_RE_STR: &str = r"(\d{44})";

fn ensure_directories(base: &Path, verbose: bool) -> io::Result<()> {
    let structure = vec![
        "CTe",
        "CTe/Mais de 6 meses",
        "CTe/Menos de 6 meses",
        "NFe",
        "NFe/Mais de 6 meses",
        "NFe/Menos de 6 meses",
        "MDFe",
        "NFCe",
    ];
    for rel in structure {
        let p = base.join(rel);
        if !p.exists() {
            fs::create_dir_all(&p)?;
            if verbose {
                println!("[DEBUG] Criado diretório: {}", p.display());
            }
        }
    }
    Ok(())
}

fn find_chave_in_name(name: &str, re: &Regex) -> Option<String> {
    re.captures(name).and_then(|cap| cap.get(1).map(|m| m.as_str().to_string()))
}

fn six_months_ago_reference(today: chrono::NaiveDate) -> chrono::NaiveDate {
    // Retorna o primeiro dia do mês que representa 6 meses atrás
    let mut year = today.year();
    let mut month = today.month() as i32; // 1..=12
    // Subtrai 6 meses
    month -= 6;
    while month <= 0 {
        month += 12;
        year -= 1;
    }
    chrono::NaiveDate::from_ymd_opt(year, month as u32, 1).unwrap()
}

fn determine_destination_for_xml(base: &Path, chave: &str, today: chrono::NaiveDate, verbose: bool) -> Option<PathBuf> {
    if chave.len() != 44 {
        if verbose {
            eprintln!("[WARN] chave com tamanho inesperado: {}", chave);
        }
        return None;
    }
    let model = &chave[20..22];
    let model_name = match model {
        "55" => "NFe",
        "57" => "CTe",
        "58" => "MDFe",
        "65" => "NFCe",
        other => {
            if verbose {
                eprintln!("[INFO] modelo desconhecido {} em chave {}", other, chave);
            }
            return None;
        }
    };

    if model == "55" || model == "57" {
        // ano = 2000 + chave[2..4]
        let yy = &chave[2..4];
        let mm = &chave[4..6];
        if let (Ok(yyv), Ok(mmv)) = (yy.parse::<i32>(), mm.parse::<u32>()) {
            let year = 2000 + yyv;
            if mmv < 1 || mmv > 12 {
                if verbose {
                    eprintln!("[WARN] mês inválido na chave {}: {}", chave, mmv);
                }
                return None;
            }
            let file_date = chrono::NaiveDate::from_ymd_opt(year, mmv, 1);
            if file_date.is_none() {
                if verbose {
                    eprintln!("[WARN] data inválida a partir da chave {}", chave);
                }
                return None;
            }
            let file_date = file_date.unwrap();
            let six_months_ago = six_months_ago_reference(today);
            let rel = if file_date < six_months_ago {
                PathBuf::from(model_name).join("Mais de 6 meses")
            } else {
                PathBuf::from(model_name).join("Menos de 6 meses")
            };
            let dest = base.join(rel);
            if !dest.exists() {
                if let Err(e) = fs::create_dir_all(&dest) {
                    eprintln!("[ERROR] falha criando diretório {}: {}", dest.display(), e);
                    return None;
                }
            }
            return Some(dest);
        } else {
            if verbose {
                eprintln!("[WARN] erro parseando ano/mes na chave {}", chave);
            }
            return None;
        }
    } else {
        // MDFe ou NFCe -> diretório raiz do modelo dentro de base
        let dest = base.join(model_name);
        if !dest.exists() {
            if let Err(e) = fs::create_dir_all(&dest) {
                eprintln!("[ERROR] falha criando diretório {}: {}", dest.display(), e);
                return None;
            }
        }
        return Some(dest);
    }
}

fn unique_dest(dest_dir: &Path, file_name: &str) -> Option<PathBuf> {
    let candidate = dest_dir.join(file_name);
    if !candidate.exists() {
        return Some(candidate);
    } 
    return None;
}

fn safe_extract_zip(zip_path: &Path, extract_to: &Path, verbose: bool) -> io::Result<Vec<PathBuf>> {
    let file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(file).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let mut extracted_paths = Vec::new();
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let name = file.name();
        if name.contains("..") || Path::new(name).is_absolute() {
            if verbose {
                eprintln!("[WARN] Ignorando entrada insegura no zip: {}", name);
            }
            continue;
        }
        let outpath = extract_to.join(name);
        if let Some(parent) = outpath.parent() {
            fs::create_dir_all(parent)?;
        }
        if file.is_dir() {
            fs::create_dir_all(&outpath)?;
        } else {
            let mut outfile = File::create(&outpath)?;
            io::copy(&mut file, &mut outfile)?;
            extracted_paths.push(outpath);
        }
    }
    Ok(extracted_paths)
}

fn copy_file_to_dest(src: &Path, dest_dir: &Path, dry_run: bool, verbose: bool) -> Option<io::Result<PathBuf>> {
    let file_name = src.file_name().and_then(|s| s.to_str()).unwrap_or("file.xml");
    let dest = unique_dest(dest_dir, file_name);
    
    match dest {
        None => None,
        Some(dest) => {
            if verbose {
                println!("[INFO] Copiando '{}' -> '{}'", src.display(), dest.display());
            }
            if dry_run  {
                return Some(Ok(dest));
            }
            Some(fs::copy(src, &dest).map(|_| dest))
        }
    }

}

fn process_root(root: &Path, base: &Path, dry_run: bool, _: usize, verbose: bool) -> io::Result<()> {
    let re = Regex::new(CHAVE_RE_STR).unwrap();
    let today = Local::now().date_naive();

    // Coleta de tarefas de cópia
    let copy_tasks = std::sync::Mutex::new(Vec::<(PathBuf, PathBuf)>::new());

    // Primeiro varre todos os arquivos
    for entry in WalkDir::new(root).follow_links(false).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path().to_path_buf();
        if path.is_file() {
            let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("").to_lowercase();
            if ext == "zip" {
                if verbose {
                    println!("[DEBUG] Processando zip: {}", path.display());
                }
                let td = tempdir()?;
                match safe_extract_zip(&path, td.path(), verbose) {
                    Ok(extracted) => {
                        for ex in extracted {
                            if ex.extension().and_then(|s| s.to_str()).map(|s| s.eq_ignore_ascii_case("xml")).unwrap_or(false) {
                                if let Some(chave) = find_chave_in_name(&ex.file_name().and_then(|s| s.to_str()).unwrap_or(""), &re) {
                                    if let Some(dest_dir) = determine_destination_for_xml(base, &chave, today, verbose) {
                                        copy_tasks.lock().unwrap().push((ex.clone(), dest_dir));
                                    }
                                } else if verbose {
                                    println!("[DEBUG] Nenhuma chave 44 dígitos em {}", ex.display());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[WARN] falha ao extrair zip {}: {}", path.display(), e);
                    }
                }
                // td é removido ao sair do escopo
            } else if ext == "xml" {
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    if let Some(chave) = find_chave_in_name(name, &re) {
                        if let Some(dest_dir) = determine_destination_for_xml(base, &chave, today, verbose) {
                            copy_tasks.lock().unwrap().push((path.clone(), dest_dir));
                        }
                    } else if verbose {
                        println!("[DEBUG] Nenhuma chave 44 dígitos em {}", path.display());
                    }
                }
            }
        }
    }

    let tasks = copy_tasks.into_inner().unwrap();
    let total = tasks.len();
    println!("[INFO] Tarefas de cópia a executar: {}", total);

    // Executa cópias em paralelo usando rayon
    tasks.par_iter().with_max_len(1).for_each(|(src, dest_dir)| {
        if let Some(Err(e)) = copy_file_to_dest(src, dest_dir, dry_run, verbose) {
            eprintln!("[ERROR] falha copiando {} -> {}: {}", src.display(), dest_dir.display(), e);
        }
    });

    println!("[INFO] Concluído: {} cópias (simulação: {})", total, dry_run);
    Ok(())
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let root = args.root.unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from("~/")));
    let default_dest = dirs::home_dir().unwrap_or_else(|| PathBuf::from("~")).join("docs_fiscais");
    let dest_base = args.dest.unwrap_or(default_dest);

    if !root.exists() {
        eprintln!("[ERROR] Diretório raiz não existe: {}", root.display());
        std::process::exit(1);
    }

    if args.verbose {
        println!("[INFO] Iniciando varredura em: {}", root.display());
        println!("[INFO] Diretório destino base: {}", dest_base.display());
    }

    ensure_directories(&dest_base, args.verbose)?;

    process_root(&root, &dest_base, args.dry_run, args.workers, args.verbose)?;

    Ok(())
}
