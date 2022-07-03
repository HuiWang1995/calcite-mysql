create schema test;

use test;
create table student
(
    pk_id varchar(32)  not null
        primary key,
    name  varchar(128) null
);

insert into test.student (pk_id, name)
values  ('1', 'wang');

commit;