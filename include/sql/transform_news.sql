"""do $$
            begin
            for n in 0..(select jsonb_array_length(info->'articles') from news where ticker=%(symbol)s)-1 loop
            update news set info = jsonb_set(info,ARRAY['articles',n::text,'datetime'], (select to_jsonb((info->'articles'->n->'publishedAt')::text::timestamp::text) from news where ticker=%(symbol)s)) where ticker=%(symbol)s;
            update news set info = jsonb_set(info, ARRAY['articles',n::text], (select info->'articles'->n  #- '{publishedAt}' from news where ticker=%(symbol)s)) where ticker=%(symbol)s;
            end loop;
            update news set info = jsonb_set(info, ARRAY['values'], (select info -> 'articles' from news where ticker=%(symbol)s)) where ticker=%(symbol)s;
            update news set info = (info - 'articles') where ticker=%(symbol)s;
            end; $$;"""