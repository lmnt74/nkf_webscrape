create table lease_properties (
    id int
    , url varchar(1000)
    , address varchar(500)
    , city varchar(50)
    , state  varchar(10)
    , region varchar(50)
    , market varchar(50)
    , submarket varchar(50)
    , total_sqft_available varchar(50)
    , spaces varchar(50)
 )

 create table sale_properties (
    id int
    , url varchar(1000)
    , address varchar(500)
    , city varchar(50)
    , state  varchar(10)
    , region varchar(50)
    , market varchar(50)
    , submarket varchar(50)
    , total_sqft_available varchar(50)
    , spaces varchar(50)
 )

 create table broker_property (
    , broker_name varchar(1000)
    , phone_number varchar(15)
    , property_id int
 )