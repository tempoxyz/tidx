use anyhow::Result;
use chrono::Utc;
use clap::Args as ClapArgs;

use ak47::db;
use ak47::service;

#[derive(ClapArgs)]
pub struct Args {
    /// Database URL
    #[arg(long, env = "AK47_DATABASE_URL")]
    pub db: String,

    /// Watch mode - continuously update status
    #[arg(long, short)]
    pub watch: bool,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let pool = db::create_pool(&args.db).await?;

    loop {
        let status = service::get_status(&pool).await?;

        if args.watch {
            print!("\x1B[2J\x1B[1;1H");
        }

        if args.json {
            println!("{}", serde_json::to_string_pretty(&status)?);
        } else {
            println!("AK47 Status");
            println!("═══════════════════════════════════════");

            match status {
                Some(s) => {
                    let age = Utc::now().signed_duration_since(s.updated_at);

                    println!("Network:    {} ({})", s.chain_name, s.chain_id);
                    println!("Head:       {}", s.head_num);
                    println!("Synced:     {}", s.synced_num);
                    println!("Lag:        {} blocks", s.lag);
                    println!(
                        "Updated:    {} ({} ago)",
                        s.updated_at.format("%H:%M:%S"),
                        format_duration(age)
                    );
                }
                None => {
                    println!("No sync state found. Run 'ak47 up' to start syncing.");
                }
            }
        }

        if !args.watch {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

fn format_duration(d: chrono::Duration) -> String {
    let secs = d.num_seconds();
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}
