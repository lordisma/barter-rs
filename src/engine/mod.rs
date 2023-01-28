use crate::{
    data::MarketGenerator,
    engine::{error::EngineError, trader::Trader},
    event::{Event, MessageTransmitter},
    execution::ExecutionClient,
    portfolio::{
        position::Position,
        repository::{PositionHandler, StatisticHandler},
        FillUpdater, MarketUpdater, OrderGenerator,
    },
    statistic::summary::{PositionSummariser, TableBuilder},
    strategy::SignalGenerator,
};
use barter_data::event::{DataKind, MarketEvent};
use barter_integration::model::{Market, MarketId};
use parking_lot::Mutex;
use prettytable::Table;
use serde::Serialize;
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Barter Engine module specific errors.
pub mod error;

/// Contains the trading event loop for a Trader capable of trading a single market pair. A Trader
/// has it's own Data handler, Strategy & Execution handler, as well as shared access to a global
/// Portfolio instance.
pub mod trader;

/// Commands that can be actioned by an [`Engine`] and it's associated [`Trader`]s.
#[derive(Debug)]
pub enum Command {
    /// Fetches all the [`Engine`]'s open [`Position`]s and sends them on the provided
    /// `oneshot::Sender`. Involves the [`Engine`] only.
    FetchOpenPositions(oneshot::Sender<Result<Vec<Position>, EngineError>>),

    /// Terminate every running [`Trader`] associated with this [`Engine`]. Involves all [`Trader`]s.
    Terminate(String),

    /// Exit every open [`Position`] associated with this [`Engine`]. Involves all [`Trader`]s.
    ExitAllPositions,

    /// Exit a [`Position`]. Uses the [`Market`] provided to route this [`Command`] to the relevant
    /// [`Trader`] instance. Involves one [`Trader`].
    ExitPosition(Market),
}

/// Lego components for constructing an [`Engine`] via the new() constructor method.
#[derive(Debug)]
pub struct EngineLego<EventTx, Statistic, Portfolio, Data, Strategy, Execution>
where
    EventTx: MessageTransmitter<Event> + Send,
    Statistic: Serialize + Send,
    Portfolio: MarketUpdater + OrderGenerator + FillUpdater + Send,
    Data: MarketGenerator<MarketEvent<DataKind>> + Send,
    Strategy: SignalGenerator + Send,
    Execution: ExecutionClient + Send,
{
    /// Unique identifier for an [`Engine`] in Uuid v4 format. Used as a unique identifier seed for
    /// the Portfolio, Trader & Positions associated with this [`Engine`].
    pub engine_id: Uuid,
    /// mpsc::Receiver for receiving [`Command`]s from a remote source.
    pub command_rx: mpsc::Receiver<Command>,
    /// Shared-access to a global Portfolio instance.
    pub portfolio: Arc<Mutex<Portfolio>>,
    /// Collection of [`Trader`] instances that can concurrently trade a market pair on it's own thread.
    pub traders: Vec<Trader<EventTx, Statistic, Portfolio, Data, Strategy, Execution>>,
    /// `HashMap` containing a [`Command`] transmitter for every [`Trader`] associated with this
    /// [`Engine`].
    pub trader_command_txs: HashMap<Market, mpsc::Sender<Command>>,
    /// Uses trading session's exited [`Position`]s to calculate an average statistical summary
    /// across all [`Market`]s traded.
    pub statistics_summary: Statistic,
}

/// Multi-threaded Trading Engine capable of trading with an arbitrary number of [`Trader`]s, one
/// for each unique [`Market`].
///
/// Each [`Trader`] operates on it's own thread and has it's own Data handler, Strategy &
/// Execution Handler, as well as shared access to a global Portfolio instance. A graceful remote
/// shutdown is made possible by sending a [`Command::Terminate`] to the Engine's broadcast::Receiver
/// termination_rx.
#[derive(Debug)]
pub struct Engine<EventTx, Statistic, Portfolio, Data, Strategy, Execution>
where
    EventTx: MessageTransmitter<Event>,
    Statistic: PositionSummariser + Serialize + Send,
    Portfolio: PositionHandler
        + StatisticHandler<Statistic>
        + MarketUpdater
        + OrderGenerator
        + FillUpdater
        + Send
        + 'static,
    Data: MarketGenerator<MarketEvent<DataKind>> + Send + 'static,
    Strategy: SignalGenerator + Send,
    Execution: ExecutionClient + Send,
{
    /// Unique identifier for an [`Engine`] in Uuid v4 format. Used as a unique identifier seed for
    /// the Portfolio, Trader & Positions associated with this [`Engine`].
    engine_id: Uuid,
    /// mpsc::Receiver for receiving [`Command`]s from a remote source.
    command_rx: mpsc::Receiver<Command>,
    /// Shared-access to a global Portfolio instance that implements [`MarketUpdater`],
    /// [`OrderGenerator`] & [`FillUpdater`].
    portfolio: Arc<Mutex<Portfolio>>,
    /// Collection of [`Trader`] instances that can concurrently trade a market pair on it's own thread.
    traders: Vec<Trader<EventTx, Statistic, Portfolio, Data, Strategy, Execution>>,
    /// `HashMap` containing a [`Command`] transmitter for every [`Trader`] associated with this
    /// [`Engine`].
    trader_command_txs: HashMap<Market, mpsc::Sender<Command>>,
    /// Uses trading session's exited [`Position`]s to calculate an average statistical summary
    /// across all [`Market`]s traded.
    statistics_summary: Statistic,
}

impl<EventTx, Statistic, Portfolio, Data, Strategy, Execution>
    Engine<EventTx, Statistic, Portfolio, Data, Strategy, Execution>
where
    EventTx: MessageTransmitter<Event> + Send + 'static,
    Statistic: PositionSummariser + TableBuilder + Serialize + Send + 'static,
    Portfolio: PositionHandler
        + StatisticHandler<Statistic>
        + MarketUpdater
        + OrderGenerator
        + FillUpdater
        + Send
        + 'static,
    Data: MarketGenerator<MarketEvent<DataKind>> + Send,
    Strategy: SignalGenerator + Send + 'static,
    Execution: ExecutionClient + Send + 'static,
{
    /// Constructs a new trading [`Engine`] instance using the provided [`EngineLego`].
    pub fn new(lego: EngineLego<EventTx, Statistic, Portfolio, Data, Strategy, Execution>) -> Self {
        info!(
            engine_id = &*format!("{}", lego.engine_id),
            "constructed new Engine instance"
        );
        Self {
            engine_id: lego.engine_id,
            command_rx: lego.command_rx,
            portfolio: lego.portfolio,
            traders: lego.traders,
            trader_command_txs: lego.trader_command_txs,
            statistics_summary: lego.statistics_summary,
        }
    }

    /// Builder to construct [`Engine`] instances.
    pub fn builder() -> EngineBuilder<EventTx, Statistic, Portfolio, Data, Strategy, Execution> {
        EngineBuilder::new()
    }

    /// Run the consumer of commands for the engines, this method will be ran into the main main
    /// method. The aim of this method is to process internal commands
    async fn process_commands(&mut self) {
        while let Some(command) = self.command_rx.recv().await {
            match command {
                Command::FetchOpenPositions(position_tx) => {
                    self.fetch_open_positions(position_tx).await;
                },
                Command::Terminate(message) => {
                    info!("Received termination command");
                    self.terminate_traders(message).await;
                    break;
                },
                Command::ExitPosition(market) => {
                    self.exit_position(market).await;
                },
                Command::ExitAllPositions => {
                    self.exit_all_positions().await;
                }
            }
        }
    }

    /// Run the trading [`Engine`]. Spawns a thread for each [`Trader`] to run on. Asynchronously
    /// receives [`Command`]s via the `command_rx` and actions them
    /// (eg/ terminate_traders, fetch_open_positions). If all of the [`Trader`]s stop organically
    /// (eg/ due to a finished [`MarketGenerator`]), the [`Engine`] terminates & prints a summary
    /// for the trading session.
    pub async fn run(mut self) {
        // Run Traders on threads & send notification when they have stopped organically
        let notify_traders_stopped = self.run_traders();

        // Action received commands from remote, or wait for all Traders to stop organically
        tokio::select! {
            result = notify_traders_stopped => {
                if let Err(error) = result {
                    error!(
                        error = &*format!("{:?}", error),
                        "Error received when running the main loop"
                    );
                }
            },

            _ = self.process_commands() => {
                // NOP
            }
        }
    }

    /// Runs each [`Trader`] it's own thread. Sends a message on the returned `mpsc::Receiver<bool>`
    /// if all the [`Trader`]s have stopped organically (eg/ due to a finished [`MarketEvent`] feed).
    fn run_traders(&mut self) -> oneshot::Receiver<()> {
        // Extract Traders out of the Engine so we can move them into threads
        let traders = std::mem::take(&mut self.traders);

        // Run each Trader instance on it's own thread
        let mut thread_handles = Vec::with_capacity(traders.len());
        for trader in traders.into_iter() {
            // Make each handler in its own thread of blocking methods
            let handle = tokio::task::spawn_blocking(move || trader.run());
            thread_handles.push(handle);
        }

        // Create channel to notify the Engine when the Traders have stopped organically
        let (notify_tx, notify_rx) = oneshot::channel();

        // Create Task that notifies Engine when the Traders have stopped organically
        tokio::spawn(async move {
            for handle in thread_handles {
                if let Err(err) = handle.await {
                    error!(
                        error = &*format!("{:?}", err),
                        "Trader thread has panicked during execution",
                    )
                }
            }

            let _ = notify_tx.send(());
        });

        notify_rx
    }

    /// Fetches all the [`Engine`]'s open [`Position`]s and sends them on the provided
    /// `oneshot::Sender`.
    async fn fetch_open_positions(
        &self,
        positions_tx: oneshot::Sender<Result<Vec<Position>, EngineError>>,
    ) {
        let open_positions = self
            .portfolio
            .lock()
            .get_open_positions(self.engine_id, self.trader_command_txs.keys())
            .map_err(EngineError::RepositoryInteractionError);

        if positions_tx.send(open_positions).is_err() {
            warn!(
                why = "oneshot receiver dropped",
                "cannot action Command::FetchOpenPositions"
            );
        }
    }

    /// Terminate every running [`Trader`] associated with this [`Engine`].
    async fn terminate_traders(&self, message: String) {
        // Firstly, exit all Positions
        self.exit_all_positions().await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // Distribute Command::Terminate to all the Engine's Traders
        for (market, command_tx) in self.trader_command_txs.iter() {
            if command_tx
                .send(Command::Terminate(message.clone()))
                .await
                .is_err()
            {
                error!(
                    market = &*format!("{:?}", market),
                    why = "dropped receiver",
                    "failed to send Command::Terminate to Trader command_rx"
                );
            }
        }
    }

    /// Exit every open [`Position`] associated with this [`Engine`].
    async fn exit_all_positions(&self) {
        for (market, command_tx) in self.trader_command_txs.iter() {
            if command_tx
                .send(Command::ExitPosition(market.clone()))
                .await
                .is_err()
            {
                error!(
                    market = &*format!("{:?}", market),
                    why = "dropped receiver",
                    "failed to send Command::Terminate to Trader command_rx"
                );
            }
        }
    }

    /// Exit a [`Position`]. Uses the [`Market`] provided to route this [`Command`] to the relevant
    /// [`Trader`] instance.
    async fn exit_position(&self, market: Market) {
        if let Some((market_ref, command_tx)) = self.trader_command_txs.get_key_value(&market) {
            if command_tx
                .send(Command::ExitPosition(market))
                .await
                .is_err()
            {
                error!(
                    market = &*format!("{:?}", market_ref),
                    why = "dropped receiver",
                    "failed to send Command::Terminate to Trader command_rx"
                );
            }
        } else {
            warn!(
                market = &*format!("{:?}", market),
                why = "Engine has no trader_command_tx associated with provided Market",
                "failed to exit Position"
            );
        }
    }
}

/// Builder to construct [`Engine`] instances.
#[derive(Debug, Default)]
pub struct EngineBuilder<EventTx, Statistic, Portfolio, Data, Strategy, Execution>
where
    EventTx: MessageTransmitter<Event>,
    Statistic: Serialize + Send,
    Portfolio: MarketUpdater + OrderGenerator + FillUpdater + Send,
    Data: MarketGenerator<MarketEvent<DataKind>> + Send,
    Strategy: SignalGenerator + Send,
    Execution: ExecutionClient + Send,
{
    engine_id: Option<Uuid>,
    command_rx: Option<mpsc::Receiver<Command>>,
    portfolio: Option<Arc<Mutex<Portfolio>>>,
    traders: Option<Vec<Trader<EventTx, Statistic, Portfolio, Data, Strategy, Execution>>>,
    trader_command_txs: Option<HashMap<Market, mpsc::Sender<Command>>>,
    statistics_summary: Option<Statistic>,
}

impl<EventTx, Statistic, Portfolio, Data, Strategy, Execution>
    EngineBuilder<EventTx, Statistic, Portfolio, Data, Strategy, Execution>
where
    EventTx: MessageTransmitter<Event>,
    Statistic: PositionSummariser + Serialize + Send,
    Portfolio: PositionHandler
        + StatisticHandler<Statistic>
        + MarketUpdater
        + OrderGenerator
        + FillUpdater
        + Send,
    Data: MarketGenerator<MarketEvent<DataKind>> + Send,
    Strategy: SignalGenerator + Send,
    Execution: ExecutionClient + Send,
{
    fn new() -> Self {
        Self {
            engine_id: None,
            command_rx: None,
            portfolio: None,
            traders: None,
            trader_command_txs: None,
            statistics_summary: None,
        }
    }

    pub fn engine_id(self, value: Uuid) -> Self {
        Self {
            engine_id: Some(value),
            ..self
        }
    }

    pub fn command_rx(self, value: mpsc::Receiver<Command>) -> Self {
        Self {
            command_rx: Some(value),
            ..self
        }
    }

    pub fn portfolio(self, value: Arc<Mutex<Portfolio>>) -> Self {
        Self {
            portfolio: Some(value),
            ..self
        }
    }

    pub fn traders(
        self,
        value: Vec<Trader<EventTx, Statistic, Portfolio, Data, Strategy, Execution>>,
    ) -> Self {
        Self {
            traders: Some(value),
            ..self
        }
    }

    pub fn trader_command_txs(self, value: HashMap<Market, mpsc::Sender<Command>>) -> Self {
        Self {
            trader_command_txs: Some(value),
            ..self
        }
    }

    pub fn statistics_summary(self, value: Statistic) -> Self {
        Self {
            statistics_summary: Some(value),
            ..self
        }
    }

    pub fn build(
        self,
    ) -> Result<Engine<EventTx, Statistic, Portfolio, Data, Strategy, Execution>, EngineError> {
        Ok(Engine {
            engine_id: self
                .engine_id
                .ok_or(EngineError::BuilderIncomplete("engine_id"))?,
            command_rx: self
                .command_rx
                .ok_or(EngineError::BuilderIncomplete("command_rx"))?,
            portfolio: self
                .portfolio
                .ok_or(EngineError::BuilderIncomplete("portfolio"))?,
            traders: self
                .traders
                .ok_or(EngineError::BuilderIncomplete("traders"))?,
            trader_command_txs: self
                .trader_command_txs
                .ok_or(EngineError::BuilderIncomplete("trader_command_txs"))?,
            statistics_summary: self
                .statistics_summary
                .ok_or(EngineError::BuilderIncomplete("statistics_summary"))?,
        })
    }
}
