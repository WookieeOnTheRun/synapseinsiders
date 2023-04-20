create schema staging;

create schema fact;

create schema dim;

-- data load pathways
create external data source dataPath
with (
    location = 'abfss://insiders@sparkmtndatalake.dfs.core.usgovcloudapi.net'
)

create external file format dataFormat
with (
    FORMAT_TYPE = PARQUET ,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

/******************************/
/* create staging load tables */
/******************************/
-- patient staging tables
-- drop external table [staging].[patientAddress] 
create external table [staging].[patientAddress]
(
    [id] varchar( 64 ) null ,
    [birthDate] varchar( 32 ) null ,
    [gender] varchar( 8 ) null ,
    [city] varchar( 32 ) null ,
    [country] varchar( 32 ) null ,
    [postalCode] varchar( 16 ) null ,
    [state] varchar( 16 ) null
) with (
    location = '/silver/patientAddress' ,
    DATA_SOURCE = dataPath ,
    FILE_FORMAT = dataFormat
)

-- drop external table [staging].[patientIdentification]
create external table [staging].[patientIdentification]
(
    [id] varchar( 64 ) null ,
    [birthDate] varchar( 32 ) null ,
    [gender] varchar( 8 ) null ,
    [maritalStatus] varchar( 512 ) null ,
    [family] varchar( 32 ) null ,
    [given] varchar( 32 ) null
) with (
    location = '/silver/patientIdentification',
    DATA_SOURCE = dataPath ,
    FILE_FORMAT = dataFormat
)

-- drop EXTERNAL table [staging].[patientExtension] 
create EXTERNAL table [staging].[patientExtension]
(
    [id] varchar( 64 ) null ,
    [birthDate] varchar( 32 ) null ,
    [gender] varchar( 8 ) null ,
    [maritalStatus] varchar( 512 ) null ,
    [url] varchar( 256 ) null ,
    [valueAddress] varchar( 256 ) null ,
    [valueDecimal] float null ,
    [valueString] varchar( 64 ) null
) with (
    location = '/silver/patientExtension/' ,
    DATA_SOURCE = dataPath ,
    FILE_FORMAT = dataFormat
)

-- claim staging tables
create external table [staging].[claimInsurance]
(
    [billablePeriod] varchar( 128 ) null ,
    [created] varchar( 32 ) null ,
    [id] varchar( 64 ) null ,
    [patientId] varchar( 64 ) null ,
    [coverage] varchar( 64 ) null ,
    [year] int null 
) with (
    location = '/silver/claimInsurance/historical/' ,
    DATA_SOURCE = dataPath ,
    FILE_FORMAT = dataFormat
)

create external table [staging].[claimDiagnosis]
(
    [created] varchar( 32 ) null ,
    [id] varchar( 64 ) null ,
    [patientId] varchar( 64 ) null ,
    [patientFirstAndLast] varchar( 96 ) null ,
    [priorityCode] varchar( 64 ) null ,
    [providerDisplay] varchar( 64 ) null ,
    [resourceType] varchar( 64 ) null ,
    [status] VARCHAR( 32 ) null ,
    [totalValue] varchar( 32 ) null ,
    [patientProductOrService] varchar( 128 ) null ,
    [year] int null
) with (
    location = '/silver/claimDiagnosis/historical/' ,
    DATA_SOURCE = dataPath ,
    FILE_FORMAT = dataFormat
)

create external table [staging].[claimProcedure]
(
    [created] varchar( 32 ) null ,
    [id] varchar( 64 ) null ,
    [patientId] varchar( 64 ) null ,
    [patientFirstAndLast] varchar( 96 ) null ,
    [priorityCode] varchar( 32 ) null ,
    [providerDisplay] varchar( 128 ) null ,
    [resourceType] varchar( 16 ) null ,
    [status] varchar( 16 ) null ,
    [totalValue] varchar( 32 ) null ,
    [patientProductOrService] varchar( 128 ) null ,
    [year] int null
) with (
    location = '/silver/claimProcedure/historical/' ,
    DATA_SOURCE = dataPath ,
    FILE_FORMAT = dataFormat
)

-- observation staging tables
-- drop external table [staging].[observationCategory]
create external table [staging].[observationCategory]
(
    [effectiveDateTime] varchar( 32 ) null ,
    [id] varchar( 64 ) null ,
    [issued] varchar( 32 ) null ,
    [resourceType] varchar( 16 ) null ,
    [status] varchar( 16 ) null ,
    [reference] varchar( 64 ) null ,
    [categoryCodeDisplay] varchar( 16 ) null
) with (
    location = '/silver/observationCategory/historical/' ,
    DATA_SOURCE = dataPath ,
    FILE_FORMAT = dataFormat
)

-- drop external table [staging].[observationCoding]
create external table [staging].[observationCoding]
(
    [effectiveDateTime] varchar( 32 ) null ,
    [id] varchar( 64 ) null ,
    [issued] varchar( 32 ) null ,
    [resourceType] varchar( 32 ) null ,
    [status] varchar( 16 ) null ,
    [reference] varchar( 64 ) null ,
    [code] varchar( 16 ) null ,
    [codeDisplay] varchar( 32 ) null
) with (
    location = '/silver/observationCoding/historical/' ,
    DATA_SOURCE = dataPath ,
    FILE_FORMAT = dataFormat
)

-- drop external table [staging].[observationValueQuantity] 
create external table [staging].[observationValueQuantity]
(
    [effectiveDateTime] varchar( 64 ) null ,
    [id] varchar( 64 ) null ,
    [issued] varchar( 32 ) null ,
    [resourceType] varchar( 64 ) null ,
    [status] varchar( 64 ) null ,
    [reference] varchar( 128 ) null ,
    [measureUnit] varchar( 16 ) null ,
    [measureValue] float null
) with (
    location = '/silver/observationValueQuantity/historical/' ,
    DATA_SOURCE = dataPath ,
    FILE_FORMAT = dataFormat
)

-- create main fact tables
-- drop table [fact].[patientMain]
create table [fact].[patientMain]
(
    [id] varchar( 64 ) null ,
    [birthDate] date null ,
    [gender] varchar( 8 ) null ,
    [maritalStatus] varchar( 512 ) null ,
    [family] varchar( 32 ) null ,
    [given] varchar( 32 ) null ,
    [city] varchar( 32 ) null ,
    [country] varchar( 32 ) null ,
    [postalCode] varchar( 16 ) null ,
    [state] varchar( 16 ) null ,
    [url] varchar( 256 ) null ,
    [valueAddress] varchar( 256 ) null ,
    [valueDecimal] varchar( 16 ) null ,
    [valueString] varchar( 64 ) null
) with (
    heap, distribution = HASH( [birthDate] )
)

insert into [fact].[patientMain]
select a.[id], cast( a.birthdate as date ) as [birthDate], a.[gender], a.[maritalStatus], a.[family], a.[given],
    b.[city], b.[country], b.[postalCode], b.[state], c.[url], c.[valueAddress], c.[valueDecimal], c.[valueString]
from [staging].[patientIdentification] a
join [staging].[patientAddress] b
on a.[id] = b.[id]
join [staging].[patientExtension] c
on a.[id] = c.[id]

-- drop table [fact].[claimsMain]
create table [fact].[claimsMain]
(
    [created] varchar( 32 ) not null ,
    [id] varchar( 64 ) not null ,
    [patientId] varchar( 64 ) null ,
    [patientFirstAndLast] varchar( 96 ) null ,
    [coverage] varchar( 64 ) null ,
    [priorityCode] varchar( 32 ) null ,
    [providerDisplay] varchar( 128 ) null ,
    [resourceType] varchar( 64 ) null ,
    [status] varchar( 32 ) null ,
    [totalValue] varchar( 32 ) null ,
    [patientProductOrService] varchar( 128 ) null ,
    [year] int null ,
    [month] int null ,
    [day] int null
) with (
    heap, distribution = HASH( [year] ), 
    partition ( year range right for values( 2017, 2018, 2019, 2020, 2021 ) )
)

insert into [fact].[claimsMain]
select cast( a.[created] as date ) as [created], a.[id], a.[patientId], a.[patientFirstAndLast], b.[coverage], a.[priorityCode], a.[providerDisplay], a.[resourceType], 
    a.[status], c.[totalValue], c.[patientProductOrService], 
    -- datepart( year, cast( a.[created] as date ) ) as [year], 
    ISNULL( a.[year], datepart( year, cast( a.[created] as date ) ) ) as 'year',
    datepart( month, cast( a.[created] as date ) ) as 'month', 
    datepart( day, cast( a.[created] as date ) ) as 'day'
from [staging].[claimDiagnosis] a
join [staging].[claimInsurance] b
on a.[id] = b.[id]
join [staging].[claimProcedure] c
on a.[id] = c.[id]

create table [fact].[observationsMain]
(
    [effectiveDateTime] varchar( 32 ) null ,
    [id] varchar( 64 ) null ,
    [issued] varchar( 32 ) null ,
    [resourceType] varchar( 32 ) null ,
    [status] varchar( 16 ) null ,
    [reference] varchar( 64 ) null ,
    [categoryCodeDisplay] varchar( 32 ) null ,
    [codeDisplay] varchar( 32 ) null ,
    [measureUnit] varchar( 16 ) null ,
    [measureValue] float null ,
    [year] int null ,
    [month] int null ,
    [day] int null
) with (
    heap, distribution = HASH( year ),
    partition( year range right for values( 2017, 2018, 2019, 2020, 2021 ) )
)

insert into [fact].[observationsMain]
select a.[effectiveDateTime], a.[id], a.[issued], a.[resourceType], a.[status], a.[reference], a.[categoryCodeDisplay], 
    b.[codeDisplay], c.[measureUnit], c.[measureValue],
    datepart( year, cast( a.[issued] as date ) ) as 'year' ,
    datepart( month, cast( a.[issued] as date ) ) as 'month' ,
    datepart( day, cast( a.[issued] as date ) ) as 'day'
from [staging].[observationCategory] a
join [staging].[observationCoding] b
on a.[id] = b.[id]
join [staging].[observationValueQuantity] c
on a.[id] = c.[id]

