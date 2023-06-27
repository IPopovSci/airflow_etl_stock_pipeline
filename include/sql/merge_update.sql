"""create or replace view {{params.view_name}} as
        (select distinct jsonb_array_elements((select info#>'{values}' from {{params.table}} where ticker=%(ticker_old)s) || (select info#>'{values}' from {{params.table}} where ticker=%(ticker)s)));
        UPDATE {{params.table}} SET info=jsonb_set(info, ARRAY['values'],(select jsonb_agg(jsonb_array_elements) from {{params.view_name}})) where ticker=%(ticker)s;
        """