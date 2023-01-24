// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use aptos_debugger::AptosDebugger;
use aptos_rest_client::Client;
use aptos_vm::AptosVM;
use clap::Parser;
use url::Url;

#[derive(Parser)]
pub struct Argument {
    #[clap(short, long)]
    endpoint: String,

    #[clap(long)]
    begin_version: u64,

    #[clap(long)]
    limit: u64,

    #[clap(long, default_value = "1")]
    concurrency_level: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    aptos_logger::Logger::new().init();
    let args = Argument::parse();
    AptosVM::set_concurrency_level_once(args.concurrency_level);

    let debugger = AptosDebugger::rest_client(Client::new(Url::parse(&args.endpoint)?))?;

    println!(
        "{:#?}",
        debugger
            .execute_past_transactions(args.begin_version, args.limit)
            .await?
    );

    Ok(())
}
