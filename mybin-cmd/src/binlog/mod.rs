mod header;
mod gtid;
mod rows_v1;
mod rows_v2;
mod query;
mod fde;
mod user_var;
mod table_map;
mod rotate;
mod intvar;
mod load;
mod rand;
mod xid;
mod incident;
mod util;
mod parser;

use crate::raw_event;
use header::{EventHeader, EventHeaderV1};
use fde::{StartData, FormatDescriptionData};
use query::QueryData;
use rotate::RotateData;
use intvar::IntvarData;
use gtid::{GtidData, PreviousGtidsData};
use user_var::UserVarData;
use load::*;
use rand::RandData;
use xid::XidData;
use incident::IncidentData;
use table_map::TableMapData;
use rows_v1::RowsDataV1;
use rows_v2::RowsDataV2;
use bytes_parser::{ReadFrom, ReadWithContext};
use bytes_parser::bytes::ReadBytes;
use bytes_parser::number::ReadNumber;
use bytes_parser::error::Result;

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
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LogEventTypeCode(pub u8);

impl From<u8> for LogEventType {
    fn from(code: u8) -> LogEventType {
        match code {
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
            _ => LogEventType::Invalid,
        }
    }
}

impl From<LogEventTypeCode> for LogEventType {
    fn from(type_code: LogEventTypeCode) -> LogEventType {
        LogEventType::from(type_code.0)
    }
}

impl From<LogEventType> for LogEventTypeCode {
    fn from(event_type: LogEventType) -> LogEventTypeCode {
        match event_type {
            LogEventType::Unknown => LogEventTypeCode(0),
            LogEventType::StartEventV3 => LogEventTypeCode(1),
            LogEventType::QueryEvent => LogEventTypeCode(2),
            LogEventType::StopEvent => LogEventTypeCode(3),
            LogEventType::RotateEvent => LogEventTypeCode(4),
            LogEventType::IntvarEvent => LogEventTypeCode(5),
            LogEventType::LoadEvent => LogEventTypeCode(6),
            LogEventType::SlaveEvent => LogEventTypeCode(7),
            LogEventType::CreateFileEvent => LogEventTypeCode(8),
            LogEventType::AppendBlockEvent => LogEventTypeCode(9),
            LogEventType::ExecLoadEvent => LogEventTypeCode(10),
            LogEventType::DeleteFileEvent => LogEventTypeCode(11),
            LogEventType::NewLoadEvent => LogEventTypeCode(12),
            LogEventType::RandEvent => LogEventTypeCode(13),
            LogEventType::UserVarEvent => LogEventTypeCode(14),
            LogEventType::FormatDescriptionEvent => LogEventTypeCode(15),
            LogEventType::XidEvent => LogEventTypeCode(16),
            LogEventType::BeginLoadQueryEvent => LogEventTypeCode(17),
            LogEventType::ExecuteLoadQueryEvent => LogEventTypeCode(18),
            LogEventType::TableMapEvent => LogEventTypeCode(19),
            LogEventType::WriteRowsEventV0 => LogEventTypeCode(20),
            LogEventType::UpdateRowsEventV0 => LogEventTypeCode(21),
            LogEventType::DeleteRowsEventV0 => LogEventTypeCode(22),
            LogEventType::WriteRowsEventV1 => LogEventTypeCode(23),
            LogEventType::UpdateRowsEventV1 => LogEventTypeCode(24),
            LogEventType::DeleteRowsEventV1 => LogEventTypeCode(25),
            LogEventType::IncidentEvent => LogEventTypeCode(26),
            LogEventType::HeartbeatLogEvent => LogEventTypeCode(27),
            LogEventType::IgnorableLogEvent => LogEventTypeCode(28),
            LogEventType::RowsQueryLogEvent => LogEventTypeCode(29),
            LogEventType::WriteRowsEventV2 => LogEventTypeCode(30),
            LogEventType::UpdateRowsEventV2 => LogEventTypeCode(31),
            LogEventType::DeleteRowsEventV2 => LogEventTypeCode(32),
            LogEventType::GtidLogEvent => LogEventTypeCode(33),
            LogEventType::AnonymousGtidLogEvent => LogEventTypeCode(34),
            LogEventType::PreviousGtidsLogEvent => LogEventTypeCode(35),
            LogEventType::TransactionContextEvent => LogEventTypeCode(36),
            LogEventType::ViewChangeEvent => LogEventTypeCode(37),
            LogEventType::XaPrepareLogEvent => LogEventTypeCode(38),
            // pseudo invalid code
            LogEventType::Invalid => LogEventTypeCode(99),
        }
    }
}

/// fast event type parsing
///
/// input must start at the beginning of an event
impl ReadFrom<'_, LogEventType> for [u8] {
    fn read_from(&self, offset: usize) -> Result<(usize, LogEventType)> {
        let (offset, _) = self.take_len(offset, 4)?;
        let (offset, type_code) = self.read_u8(offset)?;
        Ok((offset, LogEventType::from(type_code)))
    }
}

#[derive(Debug, Clone)]
pub struct EventLength(pub u32);

/// fast event length parsing
///
/// input must start at the beginning of an event
impl ReadFrom<'_, EventLength> for [u8] {
    fn read_from(&self, offset: usize) -> Result<(usize, EventLength)> {
        let (offset, _) = self.take_len(offset, 9)?;
        let (offset, event_length) = self.read_le_u32(offset)?;
        Ok((offset, EventLength(event_length)))
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
    pub crc32: u32,
}

raw_event!(StartEventV3, StartData, 'a);

pub struct FormatDescriptionEvent<'a>(FormatDescriptionData<'a>);

impl<'a> std::ops::Deref for FormatDescriptionEvent<'a> {
    type Target = FormatDescriptionData<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// implements FormatDescriptionEvent because we need to retrieve
/// checksum flag from this event
impl<'a> ReadFrom<'a, FormatDescriptionEvent<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, FormatDescriptionEvent<'a>)> {
        let (offset, header): (_, EventHeader) = self.read_from(offset)?;
        let (offset, data) = self.take_len(offset, header.data_len() as usize)?;
        let (_, fde) = data.read_from(0)?;
        Ok((offset, fde))
    }
}

raw_event!(QueryEvent, QueryData, 'a);

raw_event!(StopEvent);

raw_event!(RotateEvent, RotateData, 'a);

raw_event!(IntvarEvent, IntvarData);

raw_event!(LoadEvent, LoadData, 'a);

raw_event!(CreateFileEvent, CreateFileData, 'a);

raw_event!(AppendBlockEvent, AppendBlockData, 'a);

raw_event!(ExecLoadEvent, ExecLoadData);

raw_event!(DeleteFileEvent, DeleteFileData);

raw_event!(NewLoadEvent, NewLoadData, 'a);

raw_event!(BeginLoadQueryEvent, BeginLoadQueryData, 'a);

raw_event!(ExecuteLoadQueryEvent, ExecuteLoadQueryData, 'a);

raw_event!(RandEvent, RandData);

raw_event!(XidEvent, XidData);

raw_event!(UserVarEvent, UserVarData, 'a);

raw_event!(IncidentEvent, IncidentData, 'a);

raw_event!(HeartbeatEvent);

raw_event!(TableMapEvent, TableMapData, 'a);

raw_event!(WriteRowsEventV1, RowsDataV1, 'a);

raw_event!(UpdateRowsEventV1, RowsDataV1, 'a);

raw_event!(DeleteRowsEventV1, RowsDataV1, 'a);

raw_event!(WriteRowsEventV2, RowsDataV2, 'a);

raw_event!(UpdateRowsEventV2, RowsDataV2, 'a);

raw_event!(DeleteRowsEventV2, RowsDataV2, 'a);

raw_event!(GtidEvent, GtidData);

raw_event!(AnonymousGtidEvent, GtidData);

raw_event!(PreviousGtidsEvent, PreviousGtidsData, 'a);

pub enum Event<'a> {
    // 1
    // StartEventV3(StartEventV3<'a>),
    // 2
    QueryEvent(QueryEvent<'a>),
    // 3
    StopEvent(StopEvent),
    // 4
    RotateEvent(RotateEvent<'a>),
    // 5
    IntvarEvent(IntvarEvent),
    // 6
    LoadEvent(LoadEvent<'a>),
    // 8
    CreateFileEvent(CreateFileEvent<'a>),
    // 9
    AppendBlockEvent(AppendBlockEvent<'a>),
    // 10
    ExecLoadEvent(ExecLoadEvent),
    // 11
    DeleteFileEvent(DeleteFileEvent),
    // 12
    NewLoadEvent(NewLoadEvent<'a>),
    // 13
    RandEvent(RandEvent),
    // 14
    UserVarEvent(UserVarEvent<'a>),
    // 15
    FormatDescriptionEvent(FormatDescriptionEvent<'a>),
    // 16
    XidEvent(XidEvent),
    // 17
    BeginLoadQueryEvent(BeginLoadQueryEvent<'a>),
    // 18
    ExecuteLoadQueryEvent(ExecuteLoadQueryEvent<'a>),
    // 19
    TableMapEvent(TableMapEvent<'a>),
    // 23
    WriteRowsEventV1(WriteRowsEventV1<'a>),
    // 24
    UpdateRowsEventV1(UpdateRowsEventV1<'a>),
    // 25
    DeleteRowsEventV1(DeleteRowsEventV1<'a>),
    // 26
    IncidentEvent(IncidentEvent<'a>),
    // 27
    HeartbeatEvent(HeartbeatEvent),
    // 30
    WriteRowsEventV2(WriteRowsEventV2<'a>),
    // 31
    UpdateRowsEventV2(UpdateRowsEventV2<'a>),
    // 32
    DeleteRowsEventV2(DeleteRowsEventV2<'a>),
    // 33
    GtidEvent(GtidEvent),
    // 34
    AnonymousGtidEvent(AnonymousGtidEvent),
    // 35
    PreviousGtidsEvent(PreviousGtidsEvent<'a>),
}

