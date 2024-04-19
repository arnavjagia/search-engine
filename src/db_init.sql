
create table documents(
    url     varchar(200) primary key, -- url is not a keyword in Postgres
    content text
);

create table inverted_index(
    url         varchar(200) references documents(url),
    word        text,
    frequency   integer not null,

    constraint frequency_nonnegative check (frequency >= 0),
    primary key(url, word)
);


-- drop tuples from the inverted_index if freuency is 0

create or replace function zero_frequency_handler() returns trigger as
$$begin
    delete from inverted_index where frequency = 0;
    return null;
end;$$ language plpgsql;

create or replace trigger zero_frequency_handler
after update or insert on inverted_index
for each row
execute procedure zero_frequency_handler();


-- remove special characters and extraneous spaces

create or replace function special_char_handler() returns trigger as
$$begin
    update documents 
    set content = regexp_replace(content, '[^\w]+', ' ', 'g');
    return null;
end;$$ language plpgsql;

create or replace trigger special_char_handler
after update or insert on documents
for each row
execute procedure special_char_handler();