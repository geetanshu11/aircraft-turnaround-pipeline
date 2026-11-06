-- Normalization occurs once in Python; preserve nullable measures and UTC clocks.
select * from {{ source('raw','flights') }}
