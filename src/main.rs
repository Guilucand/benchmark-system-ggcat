use std::{
    fs::{create_dir, remove_file, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use parking_lot::Mutex;
use rand::{thread_rng, RngCore};
use structopt::StructOpt;
use sysinfo::{System, SystemExt};
use tokio::runtime::Builder;

#[derive(StructOpt)]
struct SystemTestCli {
    input_files_list: String,
    #[structopt(short, default_value = "benchmark_temp_dir")]
    temp_dir: String,
    #[structopt(short = "j", default_value = "16")]
    threads_count: usize,
}

const MB_MULT: f64 = 1024.0 * 1024.0;
const GB_MULT: f64 = 1024.0 * 1024.0 * 1024.0;

fn system_info() {
    let mut sinfo = System::new();

    sinfo.refresh_cpu();
    sinfo.refresh_memory();

    println!("Cpu: {:?}", sinfo.global_cpu_info());
    println!("Memory: {:.2}gb", sinfo.total_memory() as f64 / GB_MULT);

    println!("Usable cores: {}", num_cpus::get());
}

fn write_singlethread(file: impl AsRef<Path>, thread_id: Option<usize>) {
    const BUF_SIZE: usize = 256 * 1024;
    const SIZE: usize = 1024 * 1024 * 1024 * 2; // 2 GB write test

    let mut f = File::create(&file).unwrap();
    let mut buf = vec![77; BUF_SIZE];
    let mut tot_read = 0;
    let start = Instant::now();

    for _ in 0..(SIZE / BUF_SIZE) {
        f.write_all(&mut buf).unwrap();
        tot_read += BUF_SIZE;
    }

    let read_mb = tot_read as f64 / MB_MULT;
    let elapsed = start.elapsed().as_secs_f64();

    if thread_id.is_none() {
        println!(
            "Write {:.2}GB in {:.2?}s => {:.2}MB/s",
            read_mb,
            elapsed,
            read_mb / elapsed
        );
    } else {
        println!(
            "[THREAD {}]: Write {:.2}GB in {:.2?}s => {:.2}MB/s",
            thread_id.unwrap(),
            read_mb,
            elapsed,
            read_mb / elapsed
        );
    }
    let _ = remove_file(file);
}

fn write_random_files(files: Arc<Vec<Mutex<File>>>, thread_id: usize) {
    const BUF_SIZE: usize = 256 * 1024;
    const SIZE: usize = 1024 * 1024 * 1024 * 2; // 2 GB write test

    let mut buf = vec![77; BUF_SIZE];
    let mut tot_read = 0;
    let start = Instant::now();

    for _ in 0..(SIZE / BUF_SIZE) {
        let mut f = files[thread_rng().next_u32() as usize % files.len()].lock();

        f.write_all(&mut buf).unwrap();
        tot_read += BUF_SIZE;
    }

    let read_mb = tot_read as f64 / MB_MULT;
    let elapsed = start.elapsed().as_secs_f64();

    println!(
        "[THREAD {}]: Random write {:.2}GB in {:.2?}s => {:.2}MB/s",
        thread_id,
        read_mb,
        elapsed,
        read_mb / elapsed
    );
}

fn read_singlethread(file: impl AsRef<Path>, thread_id: Option<usize>) {
    let mut f = File::open(file).unwrap();
    let mut buf = vec![0; 4096 * 1024]; // 4MB buffering, same as ggcat input
    let mut tot_read = 0;
    let start = Instant::now();
    loop {
        let amount = f.read(&mut buf).unwrap();
        tot_read += amount;
        if amount == 0 {
            break;
        }
    }

    let read_mb = tot_read as f64 / MB_MULT;
    let elapsed = start.elapsed().as_secs_f64();

    if thread_id.is_none() {
        println!(
            "Read {:.2}GB in {:.2?}s => {:.2}MB/s",
            read_mb,
            elapsed,
            read_mb / elapsed
        );
    } else {
        println!(
            "[THREAD {}]: Read {:.2}GB in {:.2?}s => {:.2}MB/s",
            thread_id.unwrap(),
            read_mb,
            elapsed,
            read_mb / elapsed
        );
    }
}

#[tokio::main]
async fn main() {
    let args = SystemTestCli::from_args();

    println!(
        "******** INFO: running with {} threads ********",
        args.threads_count
    );

    println!("************* System info *************");
    system_info();
    let files_list = std::fs::read_to_string(args.input_files_list).unwrap();

    let files = files_list.lines().collect::<Vec<_>>();

    println!("************* Read single thread *************");
    read_singlethread(&files[0], None);

    println!("************* Read single thread again (cached) *************");
    read_singlethread(&files[0], None);

    let disk_threads = args.threads_count / 4;
    let disk_rt = Builder::new_multi_thread()
        .worker_threads(disk_threads)
        .build()
        .unwrap();
    {
        println!("************* Read multi-thread *************");
        let mut jobs = vec![];
        for i in 0..disk_threads {
            let file = files[i % files.len()].to_string();
            jobs.push(disk_rt.spawn(async move {
                read_singlethread(file, Some(i));
            }));
        }
        for job in jobs.into_iter() {
            let _ = job.await;
        }
    }

    let temp_dir = PathBuf::from(args.temp_dir);
    println!("Creating temp dir: {}", temp_dir.display());
    let _ = create_dir(&temp_dir);

    println!("************* Write single thread *************");
    let st_testfile = temp_dir.join("test-file-single-thread.dat");
    write_singlethread(&st_testfile, None);

    {
        println!("************* Write multi-thread *************");
        let mut jobs = vec![];
        for i in 0..disk_threads {
            let file = temp_dir.join(format!("test-file-multithread-{}.dat", i));
            jobs.push(disk_rt.spawn(async move {
                write_singlethread(file, Some(i));
            }));
        }
        for job in jobs.into_iter() {
            let _ = job.await;
        }
    }

    {
        println!("************* Write random multi-thread *************");
        let mut jobs = vec![];

        const FILES_COUNT: usize = 1024;
        let _ = fdlimit::raise_fd_limit();

        let files = Arc::new(
            (0..FILES_COUNT)
                .map(|i| {
                    let file = temp_dir.join(format!("test-file-random-{}.dat", i));
                    Mutex::new(File::create(file).unwrap())
                })
                .collect::<Vec<_>>(),
        );

        for i in 0..disk_threads {
            let files = files.clone();
            jobs.push(disk_rt.spawn(async move {
                write_random_files(files, i);
            }));
        }
        for job in jobs.into_iter() {
            let _ = job.await;
        }

        for i in 0..FILES_COUNT {
            let file = temp_dir.join(format!("test-file-random-{}.dat", i));
            remove_file(file).unwrap();
        }
    }

    std::thread::spawn(move || drop(disk_rt));

    let cpu_rt = Builder::new_multi_thread()
        .worker_threads(args.threads_count)
        .build()
        .unwrap();

    {
        println!("************* Cpu stress test [10s] *************");
        let mut jobs = vec![];
        let completed = Arc::new(AtomicBool::new(false));

        let start_cpu_usertime = simple_process_stats::ProcessStats::get().await.unwrap();

        for _ in 0..args.threads_count {
            let completed = completed.clone();
            jobs.push(cpu_rt.spawn(async move {
                let number = rand::random::<u128>();
                let mut result = number;
                loop {
                    if completed.load(Ordering::Relaxed) {
                        break;
                    }
                    for _ in 0..10000 {
                        result = result.wrapping_mul(result + number) + number / 177;
                    }
                }
                // To prevent compiler optimizations
                if result == 291473289247344432 {
                    println!("");
                }
            }));
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let end_cpu_usertime = simple_process_stats::ProcessStats::get().await.unwrap();
        completed.store(true, Ordering::Relaxed);

        let total_spent_usertime =
            end_cpu_usertime.cpu_time_user - start_cpu_usertime.cpu_time_user;

        for job in jobs.into_iter() {
            let _ = job.await;
        }
        println!(
            "Average cpu stress test usage: {:.2}% total: {:.2}s",
            total_spent_usertime.as_secs_f64() / 10.0 * 100.0,
            end_cpu_usertime.cpu_time_user.as_secs_f64()
        );
    }

    std::thread::spawn(move || drop(cpu_rt));

    println!("All test completed!");
}
