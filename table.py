import os
import psycopg2
import sys
from colorama import init, Fore, Back, Style
import time
import pandas
import re
import psutil

st = time.time()
pr = psutil.Process(os.getpid())
pr.nice(psutil.REALTIME_PRIORITY_CLASS)

def lst(arr, p: str) -> str:
    arrs = [f for f in arr if re.search(p, f, re.IGNORECASE)]
    arrsd = [os.path.getmtime(os.path.join(cd, f)) for f in arrs]
    return max(zip(arrs, arrsd), key = lambda x: x[1])[0]

def imp(fn: str, tn: str) -> None:
    print(f"Importing {fn}...", end = '', flush = True)
    tt = time.time()
    with open(fn, 'r', encoding = 'utf-8') as f:
        cur.copy_expert(f"COPY public.{tn} FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8', QUOTE '\"', ESCAPE '''')", f)
    conn.commit()
    print(f" - \033[3;32mcompleted!\033[0m \033[35m- {round(time.time() - tt, 3)} s.\033[0m")


init()
print('\033[1;37;42mGU loading program is started. V0.7mega\033[0m')

try:
    conn = psycopg2.connect(dbname="gu", user="secretary", password="SPbU@2025", host="127.0.0.1", port="5432", options = "-c client_encoding=utf8")
except Exception as e:
    print('\033[41mCannot establish connection with db!\nCheck, that: dbname="gu", user="secretary", password="SPbU@2025", host="127.0.0.1", port="5432", options = "-c client_encoding=utf8\033[0m', e)
    input()
    sys.exit()

try:
    if getattr(sys, 'frozen', False):
        cd = os.path.dirname(sys.executable)
    else:
        cd = os.path.dirname(os.path.abspath(__file__))
    
    files  = [f for f in os.listdir(cd) if f.endswith('.csv') or f.endswith('.xlsx')]
    filec = [f for f in files if f.endswith('.csv')]
    print(f'Located in directory: {cd}\nHave found xlsx or csv files: {files}')
    
    doc = lst(filec, 'документы_поступающих')

    exam = lst(filec, 'егэ')

    state = lst(files, 'все_заявления')

    google = lst(filec, 'все программы|google.csv')

    if google != 'google.csv':
        tt = time.time()
        print(f'\033[3;36mGoogle table file {google} is latest. Converting to correct format...\033[0m', end = '', flush = True)
    
        rf = pandas.read_csv(google, dtype = 'object', engine = 'c', sep = ',', header = None, encoding = 'utf-8', quotechar = '\"', escapechar = '\'')
        sr = [i for i in range(len(rf)) if pandas.isna(rf.iloc[i, 9])]

        rf = pandas.read_csv(google, dtype = 'object', engine = 'c', skiprows = sr, usecols = range(0, 44), sep = ',', encoding = 'utf-8', quotechar = '\"', escapechar = '\'')
        
        google = 'google.csv'
        rf.to_csv(google, mode = 'w', index = False, sep = ';', header = True, encoding = 'utf-8', quotechar = '\"', escapechar = '\'', na_rep = '')

        print(f" - \033[3;32mcompleted!\033[0m \033[35m- {round(time.time() - tt, 3)} s.\033[0m")
    
    if state.endswith('.xlsx'):
        tt = time.time()
        print(f'\033[3;36m.xlsx file {state} is latest. Converting to .csv format...\033[0m', end = '', flush = True)
    
        rf = pandas.read_excel(state, dtype = 'object', usecols = 'B, C, G:I, O, R, S:T, AA, AH, AJ:AK, AN:AP', engine = 'calamine')
        
        state = state.replace('xlsx', 'csv')
        rf.to_csv(state, mode = 'w', sep = ';', header = True, encoding = 'utf-8', index = False, quotechar = '\"', escapechar = '\'', na_rep = '')

        print(f" - \033[3;32mcompleted!\033[0m \033[35m- {round(time.time() - tt, 3)} s.\033[0m")
    
    conc = 'conc.csv'
    if not os.path.isfile(os.path.join(cd, conc)) or not os.path.isfile(os.path.join(cd, google)):
        raise Exception(f'There\'s no {conc} or {google} file!')

    print(f'Taking files: \033[3;33m{conc}, {google}, {doc}, {exam}, {state}\033[0m')
    
    cur = conn.cursor()

    cur.execute("""
    drop table if exists state, doc, exam, google, concurs_name, result, google_t cascade;

    SET datestyle = 'ISO, DMY';

    create table if not exists state
    (
        --id int,
        uuid int,
        Name text,
        --Sex text,
        --birth_date date,
        --birth_Place text,
        Phone text,
        Mail text,
        Snils text,
        --Pasp text,
        --pasp_S text,
        --pasp_n text,
        --Country text,
        --Pasp_date date,
        id_app int,
        --id_gu text,
        --Source text,
        Relevance text,
        reg_date timestamp,
        Change_date timestamp,
        --Payment_type text,
        --Entrance_type text,
        --Dormitory text,
        --Special_conditions text,
        --Music text,
        --id_kg int,
        id_k int,
        --kg_date date,
        --source_kg text,
        --direction text,
        --program text,
        --format text,
        pay text,
        --sport text,
        priority int,
        status text,
        --online_pay text,
        --line_pay text,
        line_check text,
        online_check text,
        check_place text
        --rej text,
        --enr text
    );

    create table if not exists exam
    (
        id int,
        uuid int,
        doc_id int,
        doc_type_id int,
        type text,
        status text,
        S int,
        N int,
        date date,
        organisation text,
        reg_n int,
        region text,
        subject text,
        result int,
        date2 date,
        constraint uniqueness_exam Primary Key (doc_id)
    );

    create table if not exists doc 
    (	
        id int,
        uuid int,
        doc_id int primary key,
        type_id int,
        type text,
        S text,
        N text,
        date date,
        Organisation text,
        status text,
        file text
    );

    create table if not exists concurs_name 
    (
        id_k int primary key,
        program text,
        comment text,
        code text,
        name text
    );

    create table if not exists google
    (
        Secr text,
        Status_o text,
        id_app int,
        app_status text,
        id_k int,
        change_date timestamp,
        uuid int,
        Status1C text,
        date_d text,
        Name text,
        snils text,
        att_n text,
        att_p text,
        call text,
        call_res text,
        prob int,
        conc_type text,
        program text,
        P int,
        P1 text,
        P2 text,
        P3 text,
        OP text,
        Olimp text,
        M int,
        Inf int,
        Phys int,
        Rus int,
        Ach int,
        gto text,
        att text,
        olimps text,
        other text,
        sum int,
        Lgota text,
        docs text,
        phone text,
        mail text,
        region text,
        line_check text,
        online_check text,
        color int,
        comment text,
        comment_otv text
    );


    create or replace view state_mkn as
        select * from state where id_k in
            (
                103081, 103382, 103410, 104373, 147333, 147671,
                103089, 103275, 103320, 104309, 147347,
                103108, 103312, 103422, 103582, 104479,
                103130, 103368, 103381, 103560, 104300
            )
            and reg_date <= '2025-07-25 18:00:00'::timestamp;
                
    create or replace view state_mkn_p as
        select * from state where uuid in 
                (
                    select distinct uuid from state
	                    group by uuid having max
						(
							case when id_k in
		                        (
		                            103081, 103382, 103410, 104373, 147333, 147671,
			                        103089, 103275, 103320, 104309, 147347,
			                        103108, 103312, 103422, 103582, 104479,
			                        103130, 103368, 103381, 103560, 104300
		                        )
	                        then 0 end
						) 
						is not null
                );

                
    create or replace view exam_result as
        select e.uuid, MAX(case when subject = 'Математика' then result end) as M,
            MAX(case when subject = 'Информатика и ИКТ' then result end) as Inf,
            MAX(case when subject = 'Физика' then result end) as Phys,
            MAX(case when subject = 'Русский язык' then result end) as Rus
        from exam as e inner join (select distinct uuid from state_mkn) as s on e.uuid = s.uuid 
        where subject is not null and 
            e.status = 'Подтвержден в ФИС ГИА и приема' and date2 >= '2021-01-01'
        group by e.uuid;


    create or replace view ach as
        with t as
        (
            select d.uuid, d.type, d.N, d.organisation, d.status from (select distinct uuid from state_mkn) as s left join doc as d on s.uuid = d.uuid 
            where not d.type ilike '%Диплом бакалавра%' and
                    d.type not in 
                    ('Результат ЕГЭ', 'Итоговое сочинение',
                        'Диплом о среднем профессиональном образовании',
                        'Медицинская справка', 'Удостоверение волонтера (волонтерская книжка)')
        )
        select uuid,
            (case when gto = 100 or att = 10 then 10 else 0 end) as ach,
            (case when gto = 10 then 'есть подтв'
                when gto = 0 then 'не подтв'
                else 'нет' end) as gto,
            (case when att = 10 then 'подтв'
                when att = 0 then 'не подтв'
                else 'нет' end) as att,
            (case when olimp >= 0 then 'есть'
                else 'нет' end) as olimp,	
            (case when other = 0 then 'есть'
                else 'нет' end) as other,
            (case when olimp = 100 then 'Всерос?'
                when olimp = 10 then 'I ур?'
                when olimp = 1 then 'II/III ур?'
                else '' end) as bvi,
            place, att_n, 
            (case when att_p = 'Подтвержден в ФРДО' then 'подтв'
                else 'не пров' end) as att_p
        from 
        (
            select t.uuid,
                MAX(case when type ~* '(знак гто)' then
                        case when status ~* '(Подтвержден ЕПГУ)' then 10 else 0 end
                    else null
                end) as gto,
                MAX(case when type ~* '(отличием|медал)' then
                        case when status ~* '(Подтвержден в ФРДО)' then 10 else 0 end
                    else null
                end) as att,
                MAX(case when type ~* '(олимпиад)' then
                        case when type ~* '(всерос)' then 100
                        when type ~* '(II и III уровня)' then 1
                        when type ~* '(I уровня)' then 10
                        else 0 end
                    else null 
                end) as olimp,
                MAX(case when not type ~* '(знак гто|отличием|медал|олимпиад|паспорт|аттестат)' then 0
                    else null
                end) as other,
                MAX(case when type ~* '(паспорт)' then organisation 
                    else null
                end) as place,
                MAX(case when type ~* '(аттестат)' then N
                    else null
                end) as att_n,
                MAX(case when type ~* '(аттестат)' and status ~* '(Подтвержден в ФРДО)' then status
                    else null
                end) as att_p
            from t group by t.uuid
        );


    create or replace view real_p as
        (
            select id_app, pay, id_k, 
                row_number() over (partition by id_app, pay order by priority) as rp
            from state_mkn_p where status != 'Отозвано'
        );

    create or replace view priority as
        with t as
        (
            select id_app, 'Основные места в рамках КЦП' as descr,
                MAX(case when rp = 1 then id_k end) as P1,
                MAX(case when rp = 2 then id_k end) as P2,
                MAX(case when rp = 3 then id_k end) as P3
            from real_p where pay = 'Основные места в рамках КЦП' group by id_app
        
            union all

            select id_app, 'Особая квота' as descr,
                MAX(case when rp = 1 then id_k end) as P1,
                MAX(case when rp = 2 then id_k end) as P2,
                MAX(case when rp = 3 then id_k end) as P3
            from real_p where pay = 'Особая квота' group by id_app
        
            union all

            select id_app, 'Отдельная квота' as descr,
                MAX(case when rp = 1 then id_k end) as P1,
                MAX(case when rp = 2 then id_k end) as P2,
                MAX(case when rp = 3 then id_k end) as P3
            from real_p where pay = 'Отдельная квота' group by id_app
        
            union all

            select id_app, 'Целевая детализированная квота' as descr,
                MAX(case when rp = 1 then id_k end) as P1,
                MAX(case when rp = 2 then id_k end) as P2,
                MAX(case when rp = 3 then id_k end) as P3
            from real_p where pay = 'Целевая детализированная квота' group by id_app
        
            union all

            select id_app, 'Целевая недетализированная квота' as descr,
                MAX(case when rp = 1 then id_k end) as P1,
                MAX(case when rp = 2 then id_k end) as P2,
                MAX(case when rp = 3 then id_k end) as P3
            from real_p where pay ~* 'Целевая недетализированная квота' group by id_app

            union all
            
            select id_app, 'Платные места' as descr,
                MAX(case when rp = 1 then id_k end) as P1,
                MAX(case when rp = 2 then id_k end) as P2,
                MAX(case when rp = 3 then id_k end) as P3
            from real_p where pay = 'Платные места' group by id_app
        )
        select id_app, descr, s1.name as p1, s2.name as p2, s3.name as p3 from t
        
        left join concurs_name as s1 on t.p1 = s1.id_k
        left join concurs_name as s2 on t.p2 = s2.id_k
        left join concurs_name as s3 on t.p3 = s3.id_k;


    create materialized view GU as
        select '' as Who,
            '' as Status,
            s.id_app,
            (
                case when s.status = 'Отозвано' then 'Отозвано'
                else 'Действующее'
            end) as app_status,
            s.id_k,
            (
                case when change_date is not null then date_trunc('minute', change_date)
                else date_trunc('minute', reg_date)
            end) as change_date,
            s.uuid,
            '' as Status1C,
            '' as Secret,
            s.name,
            s.snils,
            a.att_n,
            a.att_p,
            '' as Call,
            '' as Call_res,
            null::int as prob,
            (
                case when s.pay = 'Основные места в рамках КЦП' then 'общий'
                when s.pay = 'Особая квота' then 'особая квота'
                when s.pay = 'Отдельная квота' then 'отдельная квота'
                when s.pay = 'Платные места' then 'договор'
                when s.pay ~* '(Целевая)' then 'целевая квота'
            end) as pay,
            c.name as Program,
            (
                case when s.status = 'Отозвано' then s.priority
                else rp.rp
            end) as rp,
            p.p1,
            p.p2,
            p.p3,
            '' as OP,
            a.bvi,
            e.m,
            e.inf,
            e.phys,
            e.rus,
            a.ach,
            a.gto,
            a.att,
            a.olimp,
            a.other,
            null::integer as Sum,
            '' as Kvota,
            '' as Docs,
            '''' || s.phone as phone, 
            s.mail,
            '~' || a.place as Region,
            (
                case when s.line_check ~* '(ложь|false)' then 'нет'
                when s.line_check ~* '(истина|true)' then s.check_place
            end) as line_agr,
            (
                case when s.online_check ~* '(ложь|false)' then 
                    case when s.line_check ~* '(истина|true)' then 'нет'
                    when s.line_check ~* '(ложь|false)' then coalesce(s.check_place, 'нет') end
                when s.online_check ~* '(истина|true)' then s.check_place
            end) as online_agr,
            null::integer as color,
            '' as note,
            '' as note_secret
        
            from state_mkn as s
            left join exam_result as e on s.uuid = e.uuid
            left join ach as a on s.uuid = a.uuid
            left join priority as p on s.id_app = p.id_app and s.pay = p.descr and s.status != 'Отозвано'
            left join real_p as rp on s.id_app = rp.id_app and s.id_k = rp.id_k
            left join concurs_name as c on s.id_k = c.id_k;
    """)

    conn.commit()
    
    imp(conc, 'concurs_name')

    imp(google, 'google')

    imp(state, 'state')

    imp(doc, 'doc')

    imp(exam, 'exam')

    #input()

    print('\033[3mCalculating result table...', end = '', flush = True)
    tt = time.time()

    cur.execute("""
        refresh materialized view gu;
        
        update google set sum = null, phone = '''' || phone;

        update google set line_check = line_agr, online_check = online_agr from gu where google.uuid = gu.uuid;
        update google set app_status = gu.app_status from gu where google.id_app = gu.id_app and google.id_k = gu.id_k;
                    
        update google set snils = gu.snils from gu where google.snils is null and google.uuid = gu.uuid;
                    
        update google set att_n = gu.att_n, att_p = gu.att_p, ach = gu.ach, att = gu.att from gu 
            where (google.att_p = 'не пров' or google.att_p is null or google.att_n is null) and google.uuid = gu.uuid;
                    
        update google set m = gu.m from gu where google.m is null and google.uuid = gu.uuid;
        update google set inf = gu.inf from gu where google.inf is null and google.uuid = gu.uuid;
        update google set phys = gu.phys from gu where google.phys is null and google.uuid = gu.uuid;
        update google set rus = gu.rus from gu where google.rus is null and google.uuid = gu.uuid;
    """)
    conn.commit()

    cur.execute("""
        create index google_i on google using btree (id_app, id_k);

        create table result as
            select * from
            (
                select * from gu where change_date > 
                    coalesce
                    (
                        (select MAX(change_date) from google as g where g.id_app = gu.id_app and g.id_k = gu.id_k and g.status_o is not null), 
                        '2025-01-01 00:00:00'
                    )
                
                union all
                
                select * from google where google.status_o is not null
            )
            order by
                max(case when status is null or status = '' then 1 else null end) over (partition by uuid) asc,
                (case when min(rp) over (partition by uuid) = 1 then 1 else null end) asc,
                max(case when status is null or status = '' then 1
                    when status = 'нет в 1С' then 2
                    when status = 'в процессе' then 3
                    else 4
                end) over (partition by uuid) asc,
                max(change_date) over (partition by uuid) desc,
                uuid desc, id_k asc, change_date desc;
        """)
    conn.commit()

    print(f" - \033[1;4mcompleted!\033[0m \033[35m- {round(time.time() - tt, 3)} s.\033[0m")

    with open('res.csv', 'w', encoding = 'utf-8') as f:
        cur.copy_expert("COPY public.result TO STDOUT WITH (FORMAT csv, DELIMITER ';', HEADER true, ENCODING 'UTF8', QUOTE '\"', ESCAPE '''')", f)

    conn.commit()
    print(f"\n\033[1;3;4;32mres.csv is ready!\033[0m\n\nProgram finished \033[35m- total: {round(time.time() - st, 3)} s.\033[0m")

except ValueError as e:
    print("\033[31mThere\'s no \'*документы_поступающих*\', \'*все_заявления*\', \'*все программы*\', google table or \'*егэ*\' file!\033[0m", e)

except Exception as e:
    print("\033[31mError: ", e)
    print("\033[0m")
    
finally:    
    conn.close()
    input()
    sys.exit()   
