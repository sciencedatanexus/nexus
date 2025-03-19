/*
Install Schema specific tables of taxonomies
This function needs to be run at the beginning of a project, each time a schema is created
doctype_codes needs to be edited with get_db_papers.sql
*/

/*
Document types coding
Table with the codes of unique document types from WOS documents
*/
drop table if exists project.doctype_codes ;
CREATE TABLE project.doctype_codes (
		doctype_code varchar(2),
		doctype varchar(50)
	);
insert into project.doctype_codes VALUES ('AA', 'Article');
insert into project.doctype_codes VALUES ('PP', 'Proceedings paper');
insert into project.doctype_codes VALUES ('RR', 'Review');
insert into project.doctype_codes VALUES ('RP', 'Retracted paper');
insert into project.doctype_codes VALUES ('PX', 'Preprint');
insert into project.doctype_codes VALUES ('DD', 'Data');
insert into project.doctype_codes VALUES ('OO', 'Other');

/*
Document types coding
*/
drop table if exists project.ct_codes ;

CREATE TABLE project.ct_codes (
		level integer,
		id integer,
		topic_label text
	);

insert into project.ct_codes select * from cite_topics.citation_topic_lookup;
-- insert values for 0 when not CT are available
insert into project.ct_codes VALUES (1, 0, 'Other');
insert into project.ct_codes VALUES (2, 0, 'Other');
insert into project.ct_codes VALUES (3, 0, 'Other');

/*
Document categories coding
*/
drop table if exists project.categories_codes ;

CREATE TABLE project.categories_codes (
	id_source varchar(7),
    id_level smallint,
    id text,
    id_label text
	);


