mod fde;
mod gtid;
mod header;
mod incident;
mod intvar;
mod load;
mod parser;
mod query;
mod rand;
mod rotate;
mod rows_v1;
pub mod rows_v2;
mod table_map;
mod user_var;
mod util;
mod xid;

use crate::try_from_event;
use bytes::Bytes;
use bytes_parser::error::{Error, Result};
use bytes_parser::{ReadBytesExt, ReadFromBytes};
use fde::{FormatDescriptionData, StartData};
use gtid::{AnonymousGtidLogData, GtidLogData, PreviousGtidsLogData};
pub use header::{EventHeader, EventHeaderV1};
use incident::IncidentData;
use intvar::IntvarData;
use load::*;
pub use parser::{BinlogVersion, ParserV4};
use query::QueryData;
use rand::RandData;
pub use rotate::RotateData;
use rows_v1::{DeleteRowsDataV1, UpdateRowsDataV1, WriteRowsDataV1};
use rows_v2::{DeleteRowsDataV2, UpdateRowsDataV2, WriteRowsDataV2};
use std::convert::TryFrom;
use table_map::TableMapData;
use user_var::UserVarData;
use xid::XidData;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogEventType {
    Unknown,
    StartEventV3,
    QueryEvent,
    StopEvent,
    RotateEvent,
    IntvarEvent,
    LoadEvent,
    SlaveEvent,
    CreateFileEvent,
    AppendBlockEvent,
    ExecLoadEvent,
    DeleteFileEvent,
    NewLoadEvent,
    RandEvent,
    UserVarEvent,
    FormatDescriptionEvent,
    XidEvent,
    BeginLoadQueryEvent,
    ExecuteLoadQueryEvent,
    TableMapEvent,
    WriteRowsEventV0,
    UpdateRowsEventV0,
    DeleteRowsEventV0,
    WriteRowsEventV1,
    UpdateRowsEventV1,
    DeleteRowsEventV1,
    IncidentEvent,
    HeartbeatLogEvent,
    IgnorableLogEvent,
    RowsQueryLogEvent,
    WriteRowsEventV2,
    UpdateRowsEventV2,
    DeleteRowsEventV2,
    GtidLogEvent,
    AnonymousGtidLogEvent,
    PreviousGtidsLogEvent,
    TransactionContextEvent,
    ViewChangeEvent,
    XaPrepareLogEvent,
}

impl TryFrom<u8> for LogEventType {
    type Error = Error;
    fn try_from(code: u8) -> Result<LogEventType> {
        let ty = match code {
            0 => LogEventType::Unknown,
            1 => LogEventType::StartEventV3,
            2 => LogEventType::QueryEvent,
            3 => LogEventType::StopEvent,
            4 => LogEventType::RotateEvent,
            5 => LogEventType::IntvarEvent,
            6 => LogEventType::LoadEvent,
            7 => LogEventType::SlaveEvent,
            8 => LogEventType::CreateFileEvent,
            9 => LogEventType::AppendBlockEvent,
            10 => LogEventType::ExecLoadEvent,
            11 => LogEventType::DeleteFileEvent,
            12 => LogEventType::NewLoadEvent,
            13 => LogEventType::RandEvent,
            14 => LogEventType::UserVarEvent,
            15 => LogEventType::FormatDescriptionEvent,
            16 => LogEventType::XidEvent,
            17 => LogEventType::BeginLoadQueryEvent,
            18 => LogEventType::ExecuteLoadQueryEvent,
            19 => LogEventType::TableMapEvent,
            // below three are also called PreGa(Write|Update|Delete)RowsEvent
            // used in 5.1.0 ~ 5.1.17
            20 => LogEventType::WriteRowsEventV0,
            21 => LogEventType::UpdateRowsEventV0,
            22 => LogEventType::DeleteRowsEventV0,
            // below three used in 5.1.18 ~ 5.6.x
            23 => LogEventType::WriteRowsEventV1,
            24 => LogEventType::UpdateRowsEventV1,
            25 => LogEventType::DeleteRowsEventV1,
            26 => LogEventType::IncidentEvent,
            27 => LogEventType::HeartbeatLogEvent,
            28 => LogEventType::IgnorableLogEvent,
            29 => LogEventType::RowsQueryLogEvent,
            // below three used after 5.6.x
            30 => LogEventType::WriteRowsEventV2,
            31 => LogEventType::UpdateRowsEventV2,
            32 => LogEventType::DeleteRowsEventV2,
            33 => LogEventType::GtidLogEvent,
            34 => LogEventType::AnonymousGtidLogEvent,
            35 => LogEventType::PreviousGtidsLogEvent,
            // below is from source code
            // https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/binlog_event.h
            36 => LogEventType::TransactionContextEvent,
            37 => LogEventType::ViewChangeEvent,
            38 => LogEventType::XaPrepareLogEvent,
            _ => {
                return Err(Error::ConstraintError(format!(
                    "invalid event type code {}",
                    code
                )))
            }
        };
        Ok(ty)
    }
}

impl From<LogEventType> for u8 {
    fn from(event_type: LogEventType) -> u8 {
        match event_type {
            LogEventType::Unknown => 0,
            LogEventType::StartEventV3 => 1,
            LogEventType::QueryEvent => 2,
            LogEventType::StopEvent => 3,
            LogEventType::RotateEvent => 4,
            LogEventType::IntvarEvent => 5,
            LogEventType::LoadEvent => 6,
            LogEventType::SlaveEvent => 7,
            LogEventType::CreateFileEvent => 8,
            LogEventType::AppendBlockEvent => 9,
            LogEventType::ExecLoadEvent => 10,
            LogEventType::DeleteFileEvent => 11,
            LogEventType::NewLoadEvent => 12,
            LogEventType::RandEvent => 13,
            LogEventType::UserVarEvent => 14,
            LogEventType::FormatDescriptionEvent => 15,
            LogEventType::XidEvent => 16,
            LogEventType::BeginLoadQueryEvent => 17,
            LogEventType::ExecuteLoadQueryEvent => 18,
            LogEventType::TableMapEvent => 19,
            LogEventType::WriteRowsEventV0 => 20,
            LogEventType::UpdateRowsEventV0 => 21,
            LogEventType::DeleteRowsEventV0 => 22,
            LogEventType::WriteRowsEventV1 => 23,
            LogEventType::UpdateRowsEventV1 => 24,
            LogEventType::DeleteRowsEventV1 => 25,
            LogEventType::IncidentEvent => 26,
            LogEventType::HeartbeatLogEvent => 27,
            LogEventType::IgnorableLogEvent => 28,
            LogEventType::RowsQueryLogEvent => 29,
            LogEventType::WriteRowsEventV2 => 30,
            LogEventType::UpdateRowsEventV2 => 31,
            LogEventType::DeleteRowsEventV2 => 32,
            LogEventType::GtidLogEvent => 33,
            LogEventType::AnonymousGtidLogEvent => 34,
            LogEventType::PreviousGtidsLogEvent => 35,
            LogEventType::TransactionContextEvent => 36,
            LogEventType::ViewChangeEvent => 37,
            LogEventType::XaPrepareLogEvent => 38,
        }
    }
}

/// fast event type parsing
///
/// input must start at the beginning of an event
impl ReadFromBytes for LogEventType {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        input.read_len(4)?;
        let type_code = input.read_u8()?;
        Ok(LogEventType::try_from(type_code)?)
    }
}

#[derive(Debug, Clone)]
pub struct EventLength(pub u32);

/// fast event length parsing
///
/// input must start at the beginning of an event
impl ReadFromBytes for EventLength {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        input.read_len(9)?;
        let event_length = input.read_le_u32()?;
        Ok(EventLength(event_length))
    }
}

/// v1 event with payload
///
/// not implemented
#[derive(Debug, Clone)]
pub struct RawEventV1<D> {
    pub header: EventHeaderV1,
    pub data: D,
}

/// v3, v4 event with payload
#[derive(Debug, Clone)]
pub struct RawEvent<D> {
    pub header: EventHeader,
    pub data: D,
}

pub type StartEventV3 = RawEvent<StartData>;
try_from_event!(StartEventV3, StartData);

pub type FormatDescriptionEvent = RawEvent<FormatDescriptionData>;
try_from_event!(FormatDescriptionEvent, FormatDescriptionData);

pub type QueryEvent = RawEvent<QueryData>;
try_from_event!(QueryEvent, QueryData);

pub type StopEvent = RawEvent<()>;

pub type RotateEvent = RawEvent<RotateData>;
try_from_event!(RotateEvent, RotateData);

pub type IntvarEvent = RawEvent<IntvarData>;
try_from_event!(IntvarEvent, IntvarData);

pub type LoadEvent = RawEvent<LoadData>;
try_from_event!(LoadEvent, LoadData);

pub type CreateFileEvent = RawEvent<CreateFileData>;
try_from_event!(CreateFileEvent, CreateFileData);

pub type AppendBlockEvent = RawEvent<AppendBlockData>;
try_from_event!(AppendBlockEvent, AppendBlockData);

pub type ExecLoadEvent = RawEvent<ExecLoadData>;
try_from_event!(ExecLoadEvent, ExecLoadData);

pub type DeleteFileEvent = RawEvent<DeleteFileData>;
try_from_event!(DeleteFileEvent, DeleteFileData);

pub type NewLoadEvent = RawEvent<NewLoadData>;
try_from_event!(NewLoadEvent, NewLoadData);

pub type BeginLoadQueryEvent = RawEvent<BeginLoadQueryData>;
try_from_event!(BeginLoadQueryEvent, BeginLoadQueryData);

pub type ExecuteLoadQueryEvent = RawEvent<ExecuteLoadQueryData>;
try_from_event!(ExecuteLoadQueryEvent, ExecuteLoadQueryData);

pub type RandEvent = RawEvent<RandData>;
try_from_event!(RandEvent, RandData);

pub type XidEvent = RawEvent<XidData>;
try_from_event!(XidEvent, XidData);

pub type UserVarEvent = RawEvent<UserVarData>;
try_from_event!(UserVarEvent, UserVarData);

pub type IncidentEvent = RawEvent<IncidentData>;
try_from_event!(IncidentEvent, IncidentData);

pub type HeartbeatLogEvent = RawEvent<()>;

pub type TableMapEvent = RawEvent<TableMapData>;
try_from_event!(TableMapEvent, TableMapData);

pub type WriteRowsEventV1 = RawEvent<WriteRowsDataV1>;
try_from_event!(WriteRowsEventV1, WriteRowsDataV1);

pub type UpdateRowsEventV1 = RawEvent<UpdateRowsDataV1>;
try_from_event!(UpdateRowsEventV1, UpdateRowsDataV1);

pub type DeleteRowsEventV1 = RawEvent<DeleteRowsDataV1>;
try_from_event!(DeleteRowsEventV1, DeleteRowsDataV1);

pub type WriteRowsEventV2 = RawEvent<WriteRowsDataV2>;
try_from_event!(WriteRowsEventV2, WriteRowsDataV2);

pub type UpdateRowsEventV2 = RawEvent<UpdateRowsDataV2>;
try_from_event!(UpdateRowsEventV2, UpdateRowsDataV2);

pub type DeleteRowsEventV2 = RawEvent<DeleteRowsDataV2>;
try_from_event!(DeleteRowsEventV2, DeleteRowsDataV2);

pub type GtidLogEvent = RawEvent<GtidLogData>;
try_from_event!(GtidLogEvent, GtidLogData);

pub type AnonymousGtidLogEvent = RawEvent<AnonymousGtidLogData>;
try_from_event!(AnonymousGtidLogEvent, AnonymousGtidLogData);

pub type PreviousGtidsLogEvent = RawEvent<PreviousGtidsLogData>;
try_from_event!(PreviousGtidsLogEvent, PreviousGtidsLogData);

#[derive(Debug, Clone)]
pub enum Event {
    // 1
    StartEventV3(StartEventV3),
    // 2
    QueryEvent(QueryEvent),
    // 3
    StopEvent(StopEvent),
    // 4
    RotateEvent(RotateEvent),
    // 5
    IntvarEvent(IntvarEvent),
    // 6
    LoadEvent(LoadEvent),
    // 8
    CreateFileEvent(CreateFileEvent),
    // 9
    AppendBlockEvent(AppendBlockEvent),
    // 10
    ExecLoadEvent(ExecLoadEvent),
    // 11
    DeleteFileEvent(DeleteFileEvent),
    // 12
    NewLoadEvent(NewLoadEvent),
    // 13
    RandEvent(RandEvent),
    // 14
    UserVarEvent(UserVarEvent),
    // 15
    FormatDescriptionEvent(FormatDescriptionEvent),
    // 16
    XidEvent(XidEvent),
    // 17
    BeginLoadQueryEvent(BeginLoadQueryEvent),
    // 18
    ExecuteLoadQueryEvent(ExecuteLoadQueryEvent),
    // 19
    TableMapEvent(TableMapEvent),
    // 23
    WriteRowsEventV1(WriteRowsEventV1),
    // 24
    UpdateRowsEventV1(UpdateRowsEventV1),
    // 25
    DeleteRowsEventV1(DeleteRowsEventV1),
    // 26
    IncidentEvent(IncidentEvent),
    // 27
    HeartbeatLogEvent(HeartbeatLogEvent),
    // 30
    WriteRowsEventV2(WriteRowsEventV2),
    // 31
    UpdateRowsEventV2(UpdateRowsEventV2),
    // 32
    DeleteRowsEventV2(DeleteRowsEventV2),
    // 33
    GtidLogEvent(GtidLogEvent),
    // 34
    AnonymousGtidLogEvent(AnonymousGtidLogEvent),
    // 35
    PreviousGtidsLogEvent(PreviousGtidsLogEvent),
}
