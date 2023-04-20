create database synapse_insiders;

use synapse_insiders;

create schema silver;

-- silver schema objects
create external data source silverData
with (
    location = 'abfss://insiders@sparkmtndatalake.dfs.core.usgovcloudapi.net'
)

create external file format silverParquet
with (
    FORMAT_TYPE = PARQUET ,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

create external file format silverDelta
with (
    FORMAT_TYPE = DELTA
)

/*******************/
/* patient objects */
/*******************/
-- patientAddress
-- drop external table [silver].[patientAddress] 
create external table [silver].[patientAddress]
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
    DATA_SOURCE = silverData ,
    FILE_FORMAT = silverParquet
)

-- patientIdentification
-- drop external table [silver].[patientIdentification]
create external table [silver].[patientIdentification]
(
    [id] varchar( 64 ) null ,
    [birthDate] varchar( 32 ) null ,
    [gender] varchar( 8 ) null ,
    [maritalStatus] varchar( 512 ) null ,
    [family] varchar( 32 ) null ,
    [given] varchar( 32 ) null
) with (
    location = '/silver/patientIdentification',
    DATA_SOURCE = silverData ,
    FILE_FORMAT = silverParquet
)

-- patientExtension
-- drop EXTERNAL table [silver].[patientExtension] 
create EXTERNAL table [silver].[patientExtension]
(
    [id] varchar( 64 ) null ,
    [birthDate] varchar( 32 ) null ,
    [gender] varchar( 8 ) null ,
    [maritalStatus] varchar( 512 ) null ,
    [url] varchar( 256 ) null ,
    [valueAddress] varchar( 256 ) null ,
    [valueDecimal] varchar( 16 ) null ,
    [valueString] varchar( 32 ) null
) with (
    location = '/silver/patientExtension/' ,
    DATA_SOURCE = silverData ,
    FILE_FORMAT = silverParquet
)

/*****************/
/* claim objects */
/*****************/
-- claimInsurance
-- drop external table [silver].[claimInsurance]
create external table [silver].[claimInsurance]
(
    [billablePeriod] varchar( 128 ) null ,
    [created] varchar( 32 ) null ,
    [id] varchar( 64 ) null ,
    [patientId] varchar( 64 ) null ,
    [coverage] varchar( 64 ) null ,
    [year] int null ,
    [month] int null ,
    [day] int null
) with (
    location = '/silver/claimInsurance/incremental/' ,
    DATA_SOURCE = silverData ,
    FILE_FORMAT = silverDelta
)

-- claimDiagnosis
-- drop external table [silver].[claimDiagnosis]
create external table [silver].[claimDiagnosis]
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
    [year] int null ,
    [month] int null ,
    [day] int null
) with (
    location = '/silver/claimDiagnosis/incremental/' ,
    DATA_SOURCE = silverData ,
    FILE_FORMAT = silverDelta
)

-- claimProcedure
-- drop external table [silver].[claimProcedure]
create external table [silver].[claimProcedure]
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
    [year] int null ,
    [month] int null ,
    [day] int null
) with (
    location = '/silver/claimProcedure/incremental/' ,
    DATA_SOURCE = silverData ,
    FILE_FORMAT = silverDelta
)

-- verify tables
-- select top 10 * from [silver].[patientAddress]
-- select count( * ) from [silver].[patientIdentification]
-- select count( * ) from [silver].[patientExtension]
-- select top 10 * from [silver].[claimInsurance]
-- select count( * ) from [silver].[claimDiagnosis]
-- select count( * ) from [silver].[claimProcedure]

-- serverless query
select c.id as 'patient_id', count( * ) as 'claim_count'
-- from [silver].[claimInsurance] c
from (
    select value, created, id
    from [silver].[claimInsurance]
    cross apply string_split( patientId, '/' )
    where value != 'Patient'
) c
left join [silver].[patientIdentification] p
on c.value = p.id
where upper( p.[gender] ) = 'MALE'
-- and datediff( year, cast( p.[birthDate] as date ), getdate() ) >= 18
-- and datediff( year, cast( p.[birthDate] as date ), getdate() ) <= 25
-- and datepart( year, (cast( c.created as date ) ) ) > 2020
group by c.id

select patientId, value from [silver].[claimInsurance] cross apply string_split( patientId, '/' ) where value != 'Patient'