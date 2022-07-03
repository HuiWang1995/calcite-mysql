create schema test2;

use test2;
create table student2
(
    pk_id varchar(32)  not null
        primary key,
    name  varchar(128) null
);

insert into test2.student2 (pk_id, name)
values  ('2', 'lu');

insert into test2.student2 (pk_id, name)
values  ('1', 'wang');

commit;