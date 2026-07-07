import os
import psycopg2
import sys
from colorama import init
import time
import pandas
import re
import psutil

###
### Developed by Morozov Lev, SP MKN SPbU 2025-2026
###

st = time.time()
pr = psutil.Process(os.getpid())
pr.nice(psutil.REALTIME_PRIORITY_CLASS)

def lst(arr, p: str) -> str:
    try:
        arrs = [f for f in arr if re.search(p, f, re.IGNORECASE)]
        arrsd = [os.path.getmtime(os.path.join(cd, f)) for f in arrs]
        return max(zip(arrs, arrsd), key = lambda x: x[1])[0]
    except ValueError as e:
        print(f"\033[31mThere\'s no *{p}* file!\033[0m")
        raise e

def cvt_to_csv(file: str, uc) -> str:
    try:
        if file.endswith('.xlsx'):
            tt = time.time()
            print(f'\033[3;36m.xlsx file {file} is latest. Converting to .csv format...\033[0m', end = '', flush = True)
            
            rf = pandas.read_excel(file, dtype = 'object', usecols = uc, engine = 'calamine')
            
            file = file.replace('xlsx', 'csv')
            rf.to_csv(file, mode = 'w', sep = ';', header = True, encoding = 'utf-8', index = False, quotechar = '\"', escapechar = '\'', na_rep = '')

            print(f" - \033[3;32mcompleted!\033[0m \033[35m- {round(time.time() - tt, 3)} s.\033[0m")
        else:
            print(f'\033[3;36m{file} is in correct format!\033[0m')

        return file
    except Exception as e:
        print(f"\033[31mError while converting {file}! Wrong or changed format!")
        raise e

def cvt_google(google: str) -> None:
    try:
        rf = pandas.read_csv(google, dtype = 'object', engine = 'c', nrows = 1, sep = ',', header = None, encoding = 'utf-8', quotechar = '\"', escapechar = '\'')
        tt = time.time()

        if rf.iloc[0, 0].find('Фамилия') == -1:
            print(f'\033[3;36mGoogle table file {google} is latest. Converting to correct format...\033[0m', end = '', flush = True)

            rf = pandas.read_csv(google, dtype = 'object', engine = 'c', sep = ',', header = None, encoding = 'utf-8', quotechar = '\"', escapechar = '\'')
            sr = [i for i in range(len(rf)) if pandas.isna(rf.iloc[i, 10])]

            rf = pandas.read_csv(google, dtype = 'object', engine = 'c', skiprows = sr, usecols = range(0, 45), sep = ',', encoding = 'utf-8', quotechar = '\"', escapechar = '\'')
                    
            rf.to_csv(google, mode = 'w', index = False, sep = ';', header = True, encoding = 'utf-8', quotechar = '\"', escapechar = '\'', na_rep = '')

            print(f" - \033[3;32mcompleted!\033[0m \033[35m- {round(time.time() - tt, 3)} s.\033[0m")
        else:
            print(f'\033[3;36m{google} is in correct format!\033[0m')
    except Exception as e:
        print(f"\033[31mError while converting {google}! Wrong or changed format!")
        raise e

def imp(fn: str, tn: str) -> None:
    try:
        print(f"Importing {fn}...", end = '', flush = True)
        tt = time.time()
        with open(fn, 'r', encoding = 'utf-8') as f:
            cur.copy_expert(f"COPY public.{tn} FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER TRUE, ENCODING 'UTF8', QUOTE '\"', ESCAPE '''')", f)
        conn.commit()
        print(f" - \033[3;32mcompleted!\033[0m \033[35m- {round(time.time() - tt, 3)} s.\033[0m")
    except Exception as e:
        print(f"\033[31mError while importing {fn}! Wrong or changed format!")
        raise e


init()
print('\033[1;37;42mGU loading program is started. V2.888u\033[0m')

try:
    conn = psycopg2.connect(dbname="gu", user="secretary", password="SPbU_MKN_PK", host="127.0.0.1", port="5432", options = "-c client_encoding=utf8")
except Exception as e:
    print('\033[41mCannot establish connection with db!\nCheck, that: dbname="gu", user="secretary", password="SPbU_MKN_PK", host="127.0.0.1", port="5432", options = "-c client_encoding=utf8\033[0m', e)
    input()
    sys.exit()

try:
    if getattr(sys, 'frozen', False):
        cd = os.path.dirname(sys.executable)
    else:
        cd = os.path.dirname(os.path.abspath(__file__))
    
    files  = [f for f in os.listdir(cd) if (f.endswith('.csv') or f.endswith('.xlsx')) and '~$'.find(f[0]) == -1]
    filec = [f for f in files if f.endswith('.csv')]
    print(f'Located in directory: {cd}\nHave found xlsx or csv files: {files}')
    
    doc = lst(files, 'документы_поступающих')
    exam = lst(files, 'егэ')
    state = lst(files, 'все_заявления')
    google = lst(filec, 'все программы')
    conc = lst(filec, 'conc.csv')
    region = lst(filec, 'region.csv')
    school = lst(filec, 'school.csv')
    
    cvt_google(google)

    state = cvt_to_csv(state, 
                #'B, C, E, G:I, O, R, S:T, AA, AH, AJ:AK, AN:AP'
                ['Уникальный код поступающего', 'ФИО', #'Дата рождения',
                'Телефон', 'Почта', 'СНИЛС', #'Серия', 'Номер',
                'Id заявления', 'Актуальность', 'Дата регистрации', 'Дата изменения', 'Id конкурса', 'Дата добавления КГ', 'Вид мест', 'Приоритет', 'Статус', 
                'Согласие подано очно', 'Согласие подано онлайн', 'Куда подано согласие'])
    
    doc = cvt_to_csv(doc, ['Уникальный код поступающего', 'Тип документа', #'Серия',
                'Номер', 'Организация, выдавшая документ', 'Статус'])
    
    exam = cvt_to_csv(exam, ['Уникальный код поступающего', 'Тип документа', 'Статус', 'Предмет', 'Балл', 'Дата решения ГЭК'])
        
    if (time.time() - min([os.path.getmtime(os.path.join(cd, f)) for f in [google, state, doc, exam]])) > 40000:
        print('\033[41mOne file is older than others by more than 11 hours or just old files by now. Is it okay?\033[0m')
        time.sleep(3)

    print(f'Taking files: \033[3;33m{conc}, {region}, {school},\n{google}, {doc}, {exam}, {state}\033[0m')
    
    cur = conn.cursor()

    cur.execute("""
    drop table if exists concurs_name, region, school, state, doc, exam, google, result cascade;

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
        --pasp_s text,
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
        date_k timestamp,
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
        --id int,
        uuid int,
        --doc_id int,
        --doc_type_id int,
        type text,
        status text,
        --S int,
        --N int,
        --date date,
        --organisation text,
        --reg_n int,
        --region text,
        subject text,
        result int,
        date2 date
    );

    create table if not exists doc 
    (	
        --id int,
        uuid int,
        --doc_id int,
        --type_id int,
        type text,
        --S text,
        N text,
        --date date,
        Organisation text,
        status text--,
        --file text
    );

    create table if not exists concurs_name 
    (
        id_k int primary key,
        program text,
        comment text,
        code text,
        name text
    );
                
    create table if not exists region
    (
        pattern text,
        region text
    );
                
    create table if not exists school
    (
        school text
    );

    create table if not exists google
    (
        Secr text,
        Status text,
        id_app int,
        app_status text,
        id_k int,
        change_date timestamp,
        uuid int,
        Status1C text,
        StatusEPGU text,
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
        RP int,
        P1 text,
        P2 text,
        P3 text,
        OP text,
        bvi text,
        M int,
        Inf int,
        Phys int,
        Rus int,
        Ach int,
        att text,
        gto text,
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
    """)

    conn.commit()
    
    imp(conc, 'concurs_name')
    imp(region, 'region')
    imp(school, 'school')

    imp(google, 'google')

    imp(state, 'state')

    imp(doc, 'doc')

    imp(exam, 'exam')

    #input()

    print('\033[3mCalculating result table...', end = '', flush = True)
    tt = time.time()

    cur.execute("""
    create or replace view state_mkn_cut as
        select * from state where uuid in 
                (
                    select distinct uuid from state
	                    group by uuid having max
						(
							case when id_k in
		                        (
		                            22511, 22523, 22525, 22540, 165082,
                                    22547, 22558, 22561, 22563, 165325, 165466,
                                    22609, 22610, 22613,
                                    22522, 22532, 22543, 22551, 165300
		                        )
	                        then 0 end
						) 
						is not null
                );
    
    create materialized view state_l as
        select * from
	        (select *, row_number() over (partition by id_app, id_k order by date_k desc) as rn from state_mkn_cut)
        where rn = 1;

    create materialized view state_mkn as
        select * from state_l where id_k in
            (
                22511, 22523, 22525, 22540, 165082,
                22547, 22558, 22561, 22563, 165325, 165466,
                22609, 22610, 22613,
                22522, 22532, 22543, 22551, 165300
            )
            and (reg_date <= '2026-07-25 17:00:00'::timestamp or pay = 'Платные места');

    create materialized view state_mkn_id as
        select distinct uuid from state_mkn;

                
    create or replace view exam_result as
        select e.uuid, MAX(case when subject = 'Математика' then result end) as M,
            MAX(case when subject = 'Информатика' then result end) as Inf,
            MAX(case when subject = 'Физика' then result end) as Phys,
            MAX(case when subject = 'Русский язык' then result end) as Rus
        from exam as e inner join state_mkn_id as s on e.uuid = s.uuid 
        where subject is not null and 
            e.status = 'Подтвержден в ФИС ГИА и приема' and date2 >= '2022-01-01'
        group by e.uuid;


    create or replace view ach as
        with t as
        (
            select d.uuid, d.type, --d.S, 
                d.N, d.organisation, d.status from state_mkn_id as s left join doc as d on s.uuid = d.uuid 
            where not d.type ilike '%Диплом бакалавра%' and
                    d.type not in 
                    ('Результат ЕГЭ', 'Итоговое сочинение',
                        'Диплом о среднем профессиональном образовании',
                        'Медицинская справка', 'Удостоверение волонтера (волонтерская книжка)')
            union all
            
            select uuid, null, null, null, null from state_mkn_id 
        )
        select distinct on (uuid)
            uuid,
            (case when att = 10 then 10 else 0 end) as ach,
            (case when att = 10 then 'подтв'
                when att = 0 then 'не подтв'
                else 'нет' end) as att,
            (case when gto = 10 then 'есть подтв'
                when gto = 0 then 'не подтв'
                else 'нет' end) as gto,
            (case when olimp >= 0 then 'есть'
                else 'нет' end) as olimp,	
            (case when other = 0 then 'есть'
                else 'нет' end) as other,
            (case when olimp = 100 then 'всерос?'
                when olimp = 10 then 'I ур?'
                when olimp = 5 then 'II/III ур?'
                when olimp = 1 then 'иная?'
                when olimp = 0 then '??'
                else '' end) as bvi,
            coalesce(r1.region, '~' || r2.region, '≈' || initcap(lower(pasp_place))) as place,
            school, --att_place,
            
            --r1.pattern as p1, r2.pattern as p2,
            --r1.region as r1, r2.region as r2,
            
            att_n,
            (case when att_p = 'Подтвержден в ФРДО' then 'подтв'
                else 'не пров' end) as att_p
            --pasp_s, pasp_n,
        from 
        (
            select t.uuid,
                MAX(case when type ~* '(отличием|медал|цвет)' then
                        case when status ~* '(Подтвержден в ФРДО)' then 10 else 0 end
                end) as att,
                MAX(case when type ~* '(знак гто)' then
                        case when status ~* '(Подтвержден ЕПГУ)' then 10 else 0 end
                end) as gto,
                MAX(case when type ~* '(олимпиад)' then
                        case when type ~* '(всерос)' and status ~* '(Подтвержден ЕПГУ)' then 100
                        when type ~* '(II и III уровня)' and status ~* '(Подтвержден ЕПГУ)' then 5
                        when type ~* '(I уровня)' and status ~* '(Подтвержден ЕПГУ)' then 10
                        when type ~* '(в иной олимпиаде)' and status ~* '(Подтвержден ЕПГУ)' then 1
                        else 0 end
                end) as olimp,
                MAX(case when not type ~* '(знак гто|отличием|медал|цвет|олимпиад|паспорт|аттестат|о рождении)' then 0 end) as other,
                MAX(case when type ~* '(аттестат)' and status ~* '(Подтвержден в ФРДО)' then N end) as att_n,
                MAX(case when type ~* '(аттестат)' and status ~* '(Подтвержден в ФРДО)' then status end) as att_p,
                MAX(case when type ~* '(аттестат)' and status ~* '(Подтвержден в ФРДО)' then replace(organisation, 'ё', 'е') end) as att_place,
                MAX(case when type ~* '(паспорт)' and status ~* '(Подтвержден ЕПГУ)' then organisation end) as pasp_place
                --MAX(case when type ~* '(паспорт)' then s end) as pasp_s,
                --MAX(case when type ~* '(паспорт)' then n end) as pasp_n
            from t group by t.uuid
        ) as gb
            left join region as r1 on gb.att_place ~* r1.pattern
            left join region as r2 on gb.pasp_place ~* r2.pattern
            left join school as s on (substring(att_n for 3) in ('031', '032', '046', '081') or 
                    r1.region ~* 'белгород|брянск|курск|севастопол' or r2.region ~* 'белгород|брянск|курск|севастопол')
                and
                case when substring(att_n for 3) = '031' then s.school ~* 'белгород'
                    when substring(att_n for 3) = '032' then s.school ~* 'брянск'
                    when substring(att_n for 3) = '046' then s.school ~* 'курск'
                    when substring(att_n for 3) = '081' then s.school ~* 'севастопол' 
                    else true end
                and
                regexp_replace(replace(s.school, 'ё', 'е'), '([«”“"»№\\- .,\\t\\(\\)])', '', 'g') ~* ('' || regexp_replace(gb.att_place, '([«”“"»№\\- .,\\t\\(\\)])', '', 'g') || '') --Эта контакенация пустоты ускоряет выполнение в 5 раз!!!
            order by uuid asc, place asc;


    create or replace view real_p as
        (
            select id_app, pay, id_k, 
                row_number() over (partition by id_app, pay order by priority) as rp
            from state_l where status != 'Отозвано'
        );

    create or replace view priority as
        with t as
        (
            select id_app, pay,
                MAX(case when rp = 1 then id_k end) as P1,
                MAX(case when rp = 2 then id_k end) as P2,
                MAX(case when rp = 3 then id_k end) as P3
            from real_p group by id_app, pay
        )
        select id_app, pay, s1.name as p1, s2.name as p2, s3.name as p3 from t
        
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
            s.status as statusEPGU,
            '' as Secret,
            initcap(lower(s.name)) as name,
            --s.birth_date,
            s.snils,
            a.att_n,
            a.att_p,
            --a.att_o,
            --a.pasp_s || ' ' || a.pasp_n,
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
            a.att,
            a.gto,
            a.olimp as olimps,
            a.other,
            null::integer as Sum,
            (case when school is not null or att_n ~ '^16[2-5]' or place ~* 'луганск|донецк|херсон|запорожье' then 'Отд Кат'
                else '' end) as Kvota,
            (case when school is not null or att_n ~ '^16[2-5]' or place ~* 'луганск|донецк|херсон|запорожье' then 'пригр школа!'
                else '' end) as Docs,
            '''' || s.phone as phone, 
            lower(s.mail) as mail,
            a.place as Region,
            (
                case when s.line_check ~* '(ложь|false)' then 'нет'
                when s.line_check ~* '(истина|true)' then s.check_place
            end) as line_check,
            (
                case when s.online_check ~* '(ложь|false)' then 
                    case when s.line_check ~* '(истина|true)' then 'нет'
                    when s.line_check ~* '(ложь|false)' then coalesce(s.check_place, 'нет') end
                when s.online_check ~* '(истина|true)' then s.check_place
            end) as online_check,
            null::integer as color,
            '' as note,
            '' as note_secret
        
            from state_mkn as s
            left join exam_result as e on s.uuid = e.uuid
            left join ach as a on s.uuid = a.uuid
            left join priority as p on s.id_app = p.id_app and s.pay = p.pay and s.status != 'Отозвано'
            left join real_p as rp on s.id_app = rp.id_app and s.id_k = rp.id_k
            left join concurs_name as c on s.id_k = c.id_k;       
    """)
    conn.commit()
    print(f" \033[35m- {round(time.time() - tt, 3)} s.\033[0m", end = '', flush = True)

    tt = time.time()

    cur.execute("""
        --create index google_i on google using btree (id_app, id_k);

        create table result as
            select * from
            (
                select 
                    (
                        case when gu.change_date = t.change_date then t.secr
                        else ''
                    end) as secr,
                    (
                        case when gu.change_date > t.change_date then
                            (
                                case when gu.status = 'Отозвано' and t.status != 'Отозвано' then 'изменено (отзыв)'
                                when gu.rp != t.rp or gu.p1 != t.p1 or gu.p2 != t.p2 or gu.p3 != t.p3 then 'изменено (П)'
                                when gu.ach > t.ach or (gu.att = 'подтв' and t.att != 'подтв'
                                        or gu.att = 'не подтв' and t.att = 'нет') 
                                    or (gu.gto = 'есть подтв' and t.gto != 'есть подтв' and t.gto != 'подтв'
                                            or gu.gto = 'не пров' and t.gto = 'нет')
                                    or (gu.olimps = 'есть' and t.olimps = 'нет')
                                    or (gu.other = 'есть' and t.other = 'нет')
                                    or (t.bvi is null and gu.bvi != '')
                                        then 'изменено (ИД)'
                                else 'изменено (?)'
                            end)
                        when gu.change_date = t.change_date then t.status
                        else ''
                    end) as status,
                    gu.id_app,
                    gu.app_status,
                    gu.id_k,
                    gu.change_date,
                    gu.uuid,
                    t.Status1C,
                    gu.statusEPGU,
                    t.date_d,
                    gu.Name,
                    gu.snils,
                    gu.att_n,
                    (
                        case when t.att_p = 'подтв' then t.att_p
                        else gu.att_p
                    end),
                    t.call,
                    t.call_res,
                    t.prob,
                    gu.pay,
                    gu.program,
                    gu.rp,
                    gu.P1,
                    gu.P2,
                    gu.P3,
                    t.op,
                    (
                        case when gu.change_date = t.change_date and t.status is not null or t.bvi is not null then t.bvi
                        else gu.bvi
                    end) as bvi,
                    (
                        case when t.M != 100 or t.M is null then gu.M
                        else 100
                    end) as M,
                    (
                        case when t.Inf != 100 or t.Inf is null then gu.Inf
                        else 100
                    end) as Inf,
                    (
                        case when t.Phys != 100 or t.Phys is null then gu.Phys
                        else 100
                    end) as Phys,
                    (
                        case when t.Rus != 100 or t.Rus is null then gu.Rus
                        else 100
                    end) as Rus,
                    (
                        case when t.ach = 10 then 10
                        else gu.ach
                    end) as ach,
                    (
                        case when t.att = 'подтв' then 'подтв'
                        else gu.att
                    end) as att,
                    (
                        case when t.gto = 'подтв' then 'подтв'
                        else gu.gto
                    end) as gto,
                    (
                        case when t.olimps != 'есть' and t.olimps != 'нет' then t.olimps
                        else gu.olimps
                    end) as olimps,
                    (
                        case when t.other != 'есть' and t.other != 'нет' then t.other
                        else gu.other
                    end) as other,
                    null::int as sum,
                    (case when t.lgota is null then gu.kvota
                        else t.lgota end) as lgota,
                    (case when t.docs is null then gu.docs
                        else t.docs end),
                    gu.phone,
                    gu.mail,
                    gu.region,
                    gu.line_check,
                    gu.online_check,
                    (case when gu.kvota = 'Отд Кат' and t.color is null then 1
                        else t.color end),
                    t.comment,
                    t.comment_otv 
                
                from gu left join google as t on t.status is not null and gu.id_app = t.id_app and gu.id_k = t.id_k
            )
            order by
                max(case when status is null or status = '' or status ~* '(изменено|в процессе|нет в)' then 1 else null end) over (partition by uuid) asc,
                (case when min(rp) over (partition by uuid) = 1 then 1 else null end) asc,

                --max(case when status is null or status = '' then 1
                --    when status ~* '(изменено|в процессе|нет в)' then 2
                --end) over (partition by uuid) desc,

                max(change_date) over (partition by uuid) asc,
                uuid desc,
                pay desc, rp asc;
        """)
    conn.commit()

    print(f" - \033[1;4mcompleted!\033[0m \033[35m- {round(time.time() - tt, 3)} s.\033[0m")

    with open('res.csv', 'w', encoding = 'utf-8') as f:
        cur.copy_expert("COPY public.result TO STDOUT WITH (FORMAT csv, DELIMITER ';', HEADER true, ENCODING 'UTF8', QUOTE '\"', ESCAPE '''')", f)

    conn.commit()
    print(f"\n\033[1;3;4;32mres.csv is ready!\033[0m\n\nProgram finished \033[35m- total: {round(time.time() - st, 3)} s.\033[0m")
    cur.close()

except Exception as e:
    print("\033[31mError: ", e)
    print("\033[0m")
    
finally:
    conn.close()
    input()
    sys.exit()   
