with stacked as (

    (
        select
            _plb_uuid,
            _plb_loaded_at,
            'my_second_dbt_model' as _plb_source_model,
            ((
                case
                    when
                        "my_second_dbt_model"."ID"::string is null
                        or "my_second_dbt_model"."ID"::string::string = ''
                        then null
                    else "my_second_dbt_model"."ID"::string
                end
            )) as "ID"
        from {{ ref('my_second_dbt_model') }} as "my_second_dbt_model"

    )

),


transformed as (

    select
        -- Generate a new deterministic UUID unique to this particular model
        _plb_loaded_at,
        'plb:::m:kansas::r:' || sha1(_plb_uuid) as _plb_uuid,
        try_cast(("ID")::string as varchar) as "ID"
    from stacked


)



select * from transformed
