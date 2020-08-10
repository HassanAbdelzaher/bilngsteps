CREATE OR REPLACE PACKAGE BODY HCS_EDAMS.billing_steps
AS
   PROCEDURE posthhdata (
      p_strbillgroup   IN   NVARCHAR2,
      p_nbillcycleid   IN   NUMBER,
      p_strcurruser    IN   NVARCHAR2,
      p_nprocessid     IN   NUMBER
   )
   AS
      strsql           VARCHAR2 (4000);
      ballowzerocons   NUMBER (1);
      ballowlowcons    NUMBER (1);
      ballowhighcons   NUMBER (1);
      berror           NUMBER (1);
      strhhtblname     NVARCHAR2 (64);
      strerror         NVARCHAR2 (128);
      ncontinue        NUMBER (10);
      nqty             NUMBER (10);
      nexists          NUMBER (10);
      ncount           NUMBER (10);
      nincrement       NUMBER (10);
      ntotrecords      NUMBER (10);
      nMinReadingNo    NUMBER (10);
   BEGIN
      progress.start_of_process (p_nprocessid, 7 /* number of phases */);


      /* --------------------------------------------------------------------------------- */
      /* ---- Phase 1: Initialisations                                                ---- */
      /* --------------------------------------------------------------------------------- */
      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      SELECT COUNT (1)
        INTO nexists
        FROM metr_rdg
       WHERE ins_cycle_id = p_nbillcycleid OR upd_cycle_id = p_nbillcycleid;

      IF nexists > 0
      THEN
         strerror :=
               'Billing cycle '
            || p_nbillcycleid
            || ' already present in METR_RDG';
         raise_application_error (-20999, strerror);
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

      SELECT COUNT (1)
        INTO nexists
        FROM metr_rdg
       WHERE reading_no IS NULL;

      IF nexists > 0
      THEN
         raise_application_error
                            (-20998,
                             'There are NULLs as reading numbers in METR_RDG'
                            );
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF; /* Read relevant settings */

      berror := 0;

      SELECT keyword_value
        INTO ballowzerocons
        FROM SETTINGS
       WHERE keyword = 'MR_SETTINGS_suspect_consumption_0';

      IF ballowzerocons IS NULL
      THEN
         berror := 1;
      END IF;

      SELECT keyword_value
        INTO ballowlowcons
        FROM SETTINGS
       WHERE keyword = 'MR_SETTINGS_suspect_consumption_1';

      IF ballowlowcons IS NULL
      THEN
         berror := 1;
      END IF;

      SELECT keyword_value
        INTO ballowhighcons
        FROM SETTINGS
       WHERE keyword = 'MR_SETTINGS_suspect_consumption_2';

      IF ballowhighcons IS NULL
      THEN
         berror := 1;
      END IF;

      IF berror = 1
      THEN
         raise_application_error
                     (-20997,
                      'Settings for suspect consumption not properly defined'
                     );
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

      strhhtblname := 'MHHREADINGS_' || UPPER (p_strbillgroup);

      SELECT COUNT (1)
        INTO nexists
        FROM all_tables
       WHERE owner = 'HCS_EDAMS' AND table_name = strhhtblname;

      IF nexists = 0
      THEN
         strerror := strhhtblname || ' table does not exist';
         raise_application_error (-20996, strerror);
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

      SELECT COUNT (1)
        INTO nexists
        FROM all_tables
       WHERE owner = 'HCS_EDAMS' AND table_name = 'CONN_DISCR';

      IF nexists = 0
      THEN
         raise_application_error (-20995, 'CONN_DISCR table does not exist.');
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

      /* --------------------------------------------------------------------------------- */
      /* ---- Phase 2: Insert new meter readings with temporary reading number 999999 ---- */
      /* --------------------------------------------------------------------------------- */
      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF (ncontinue = 0)
      THEN
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

      strsql :=
            'INSERT INTO METR_RDG( METER_TYPE, METER_REF, READING_NO, CUR_DATE, CUR_TIME,
              B_READING, READ_TYPE, READ_REASN, READ_MTHD, CLOCK_OVER, READ_CMNT, IS_INVOICD, INS_CYCLE_ID,
              UPD_CYCLE_ID )
    SELECT DISTINCT METER_TYPE, METER_REF, 999999, MIN(CR_DATE), MIN(CR_TIME), MIN(CR_READING), 0, 0, 1, MIN(CLOCK_OVER),
           MIN(MR_MSGCODE), 0, '
         || p_nbillcycleid
         || ', 0
    FROM '
         || strhhtblname
         || ' HH
    WHERE HH.DESCREPANCY = 0
          OR ( ( HH.DESCREPANCY = 6 OR HH.DESCREPANCY = 9 )
            AND '
         || ballowzerocons
         || ' = 1 )
          OR ( ( HH.DESCREPANCY = 7 OR HH.DESCREPANCY = 10 )
            AND '
         || ballowlowcons
         || ' = 1 )
          OR ( ( HH.DESCREPANCY = 8 OR HH.DESCREPANCY = 11 )
            AND '
         || ballowhighcons
         || ' = 1 )
     GROUP BY METER_TYPE, METER_REF';

      BEGIN
         EXECUTE IMMEDIATE strsql;
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error (-20994,
                                     'Error inserting new meter readings'
                                    );
            ROLLBACK;
            progress.end_of_process (p_nprocessid);
            RETURN;
      END;

      /* --------------------------------------------------------------------------------- */
      /* ---- Phase 3: Delete new readings that are already in METR_RDG (duplicates)  ---- */
      /* ----       and new readings that are older than already invoiced reading(s)  ---- */
      /* --------------------------------------------------------------------------------- */
      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF (ncontinue = 0)
      THEN
         ROLLBACK;
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

    /* Looks like there is no need to filter out only readings after
        the last installation reading                                    */

      BEGIN
         DELETE FROM metr_rdg
         WHERE reading_no = 999999
           AND ins_cycle_id = p_nbillcycleid
           AND EXISTS (
                        SELECT 1
                          FROM metr_rdg rdg2
                         WHERE rdg2.reading_no <> 999999
                           AND rdg2.meter_type = metr_rdg.meter_type
                           AND rdg2.meter_ref = metr_rdg.meter_ref
                           AND (   (    rdg2.cur_date = metr_rdg.cur_date
                                    AND ABS (  rdg2.b_reading
                                             - metr_rdg.b_reading
                                            ) < 0.01
                                   )
                                OR (    rdg2.cur_date > metr_rdg.cur_date
                                    AND rdg2.is_invoicd = 1
                                   )
                               ));
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error
               (-20993,
                'Error deleting duplicates and faulty readings from METR_RDG'
               );
            ROLLBACK;
            progress.end_of_process (p_nprocessid);
            RETURN;
      END;

      /* --------------------------------------------------------------------------------- */
      /* ---- Phase 4: Delete duplicate entries (highly unlikely)                     ---- */
      /* --------------------------------------------------------------------------------- */
      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF (ncontinue = 0)
      THEN
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

      FOR duprows IN (SELECT   meter_type, meter_ref, reading_no,
                               COUNT (*) qty
                          FROM metr_rdg
                         WHERE reading_no = 999999
                      GROUP BY meter_type, meter_ref, reading_no
                        HAVING COUNT (*) > 1)
      LOOP
         nqty := duprows.qty;

         WHILE (nqty > 1)
         LOOP
            UPDATE metr_rdg
               SET reading_no = duprows.reading_no + (nqty - 1)
             WHERE meter_type = duprows.meter_type
               AND meter_ref = duprows.meter_ref
               AND reading_no = duprows.reading_no
               AND ROWNUM = 1;

            nqty := nqty - 1;
         END LOOP;
      END LOOP;


      /* --------------------------------------------------------------------------------- */
      /* ---- Phase 5: Shift all readings that are later than current ones            ---- */
      /* ----          and assign new reading No                                      ---- */
      /* --------------------------------------------------------------------------------- */
      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF (ncontinue = 0)
      THEN
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

      select count(1) into ntotrecords
      from METR_RDG
      where READING_NO = 999999
        and INS_CYCLE_ID = p_nbillcycleid;

      progress.initialise_record (p_nprocessid, ntotrecords);
      commit;

      /* initialise counters and variables */
      ncount := 0;
      nincrement := ntotrecords / 50;
      if nincrement < 1 then nincrement := 1; end if;

      for cur_rdg in (select METER_TYPE, METER_REF, CUR_DATE
                      from METR_RDG
                      where READING_NO = 999999
                        and INS_CYCLE_ID = p_nbillcycleid
                      order by METER_TYPE, METER_REF)
      loop
         /* update progress monitor */
         if ncount = nincrement then
            progress.start_new_record (p_nprocessid,
                                       cur_rdg.METER_TYPE || '/' || cur_rdg.METER_REF,
                                       nincrement);
            commit;
            ncount := 0;
         end if;
         ncount := ncount + 1;

         select coalesce( min( READING_NO ), 0) into nMinReadingNo
         from vwMETR_RDG
         where METER_TYPE = cur_rdg.METER_TYPE
           and METER_REF  = cur_rdg.METER_REF
           and READING_NO <> 999999
           and (CUR_DATE  <= cur_rdg.CUR_DATE or
                coalesce(IS_INVOICD, 0) = 1 );

         /* Shift subsequent Readings */
         update METR_RDG
            set READING_NO = READING_NO - 1
          where METER_TYPE = cur_rdg.METER_TYPE
            and METER_REF  = cur_rdg.METER_REF
            and READING_NO < nMinReadingNo;

          /* Assign reading no to new reading */
          update METR_RDG MR
          set MR.READING_NO = nMinReadingNo - 1
          where METER_TYPE = cur_rdg.METER_TYPE
            and METER_REF  = cur_rdg.METER_REF
            and MR.READING_NO = 999999
            and MR.INS_CYCLE_ID = p_nbillcycleid;

          commit;
      end loop;


      /* --------------------------------------------------------------------------------- */
      /* ---- Phase 6: Update meter details                                           ---- */
      /* --------------------------------------------------------------------------------- */
      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF (ncontinue = 0)
      THEN
         ROLLBACK;
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

      strsql :=
            'UPDATE METR_DTL SET ( OP_STATUS, CONDITION, SERIAL_NO, NO_DIALS ) = ( select
          CASE nvl( ACC.METER_OP_STATUS, -1 )
          WHEN -1 THEN
            CASE nvl( MRN.METER_OP_STATUS, -1 )
            WHEN -1 THEN
              METR_DTL.OP_STATUS
            ELSE
              MRN.METER_OP_STATUS
            END
          ELSE
            ACC.METER_OP_STATUS
          END,
          CASE nvl(ACC.F_METER_CONDITION, N''NULLVALUE'')
          WHEN N''NULLVALUE'' THEN
            CASE nvl(MRN.F_METER_CONDITION, N''NULLVALUE'')
            WHEN N''NULLVALUE'' THEN
              METR_DTL.CONDITION
            ELSE
              MRN.F_METER_CONDITION
            END
          ELSE
            ACC.F_METER_CONDITION
          END,
          HH.SERIAL_NO,
          HH.NO_DIALS FROM '
         || strhhtblname
         || ' HH
    LEFT JOIN DESCREPANCIES_NOACCESS ACC ON HH.ACCES_CODE = ACC.CODE
    LEFT JOIN DESCREPANCIES_MRNOTES  MRN ON HH.MR_MSGCODE = MRN.CODE
    WHERE HH.METER_TYPE = METR_DTL.METER_TYPE
      AND HH.METER_REF  = METR_DTL.METER_REF
      and rownum = 1)
    WHERE exists ( select 1 from '
         || strhhtblname
         || ' HH2 where HH2.METER_TYPE = METR_DTL.METER_TYPE
      AND HH2.METER_REF  = METR_DTL.METER_REF )';

      BEGIN
         EXECUTE IMMEDIATE (strsql);
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error (-20990, 'Error updating meter details');
            ROLLBACK;
            progress.end_of_process (p_nprocessid);
            RETURN;
      END;

      /* --------------------------------------------------------------------------------- */
      /* ---- Phase 7: Insert new records into CONN_DISCR                             ---- */
      /* --------------------------------------------------------------------------------- */
      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF (ncontinue = 0)
      THEN
         ROLLBACK;
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF;

      strsql :=
            'INSERT INTO CONN_DISCR( PROP_REF, CONN_NO, CYCLE_ID, METER_TYPE, METER_REF,
      BOOK_NO, WALK_NO, SEQ_NO, DISCREPANCY, IS_POSTED, CR_READING, CR_DATE, PR_READING,
      PR_DATE, MREADER_ID, ACCESS_CODE, NOTE_CODE, ACTIONREQ_POSTED, STAMP_DATE, STAMP_USER )
    SELECT HH.PROP_REF, HH.CONN_NO, '
         || p_nbillcycleid
         || ', HH.METER_TYPE, HH.METER_REF,
      HH.BOOK_NO, HH.WALK_NO, HH.SEQNCE_NO, HH.DESCREPANCY, CASE NVL( RDG1.METER_REF, N''x'' )
        WHEN N''x'' THEN 0 ELSE 1 END, HH.CR_READING, HH.CR_DATE, CASE NVL( RDG1.METER_REF, N'''' )
        WHEN N'''' THEN HH.PR_READ1 ELSE RDG2.B_READING END, CASE NVL( RDG1.METER_REF, N'''' )
        WHEN N'''' THEN HH.PR_DATE1 ELSE RDG2.CUR_DATE END, HH.MREADER_ID, /* Change this */
      HH.ACCES_CODE, HH.MR_MSGCODE, HH.ACTIONREQ_POSTED, SYSDATE, '''
         || p_strcurruser
         || '''
    FROM '
         || strhhtblname
         || ' HH
    LEFT JOIN METR_RDG RDG1
    ON RDG1.INS_CYCLE_ID = '
         || p_nbillcycleid
         || '
      AND RDG1.METER_TYPE = HH.METER_TYPE
      AND RDG1.METER_REF  = HH.METER_REF
      AND RDG1.CUR_DATE   = HH.CR_DATE
      AND ABS( RDG1.B_READING - HH.CR_READING ) < 0.001
    LEFT JOIN METR_RDG RDG2
    ON RDG2.METER_TYPE = HH.METER_TYPE
    AND RDG2.METER_REF= HH.METER_REF
    AND RDG2.READING_NO = RDG1.READING_NO + 1';

      BEGIN
         EXECUTE IMMEDIATE strsql;
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error (-20989,
                                     'Inserting records to CONN_DISCR failed'
                                    );
            ROLLBACK;
            progress.end_of_process (p_nprocessid);
            RETURN;
      END;

      progress.end_of_process (p_nprocessid);
      COMMIT;
   END posthhdata;

  PROCEDURE CREATEHHDATA
  (
    p_strBillGroup   in NVARCHAR2,
    p_nCurrBillCycle in number,
    p_strCurrentUser in NVARCHAR2,
    p_nProcessId     in number
  )
  AS
    strHHTblName NVARCHAR2(22);
    strErr       NVARCHAR2(100);
    strSQL       VARCHAR2(4000);
    strSQL2      VARCHAR2(4000);
    nRetVal      NUMBER(10);
    nExists      number(10);
    nCnt         number(10);

    nContinue   number(10);
    nTotRecords number(10);
    nCount      number(10);
    nIncrement  number(10);

  begin
    PROGRESS.START_OF_PROCESS( p_nProcessId, 7 );
    PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nRetval );
    COMMIT;

    IF ( length( p_strBillGroup ) = 0 OR p_strBillGroup IS NULL ) then
      raise_application_error( -20988, 'Billing group must not be empty' );
      PROGRESS.END_OF_PROCESS( p_nProcessId );
      return;
    end if;

    /* If billing cycle ID is not positive, return */
    IF ( p_nCurrBillCycle <= 0 OR p_nCurrBillCycle IS NULL ) then
      raise_application_error( -20987, 'Billing cycle ID must be positive' );
      PROGRESS.END_OF_PROCESS( p_nProcessId );
      return;
    end if;

    /* If current user name is empty, return */
    IF ( length( p_strCurrentUser ) = 0 OR p_strCurrentUser IS NULL ) then
      raise_application_error( -20986, 'Current user must not be empty' );
      PROGRESS.END_OF_PROCESS( p_nProcessId );
      return;
    end if;

    PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nRetval );
    COMMIT;

    IF ( nRetval = 0 ) then
      PROGRESS.END_OF_PROCESS( p_nProcessId );
      return;
    end if;

    /* If hand held readings table doesnt exist, stop */
    strHHTblName := 'MHHREADINGS_' || UPPER( p_strBillGroup );
    select count(*) into nExists from USER_TABLES where TABLE_NAME = strHHTblName;

    IF nExists = 0 then
      begin
        Edams.createEdamsTable( 'HCS_EDAMS', 'MHHREADINGS', strHHTblName, 'HCS_EDAMS' );
      exception
        when OTHERS then
          strErr := 'Error creating ' || strHHTblName || ' table';
          raise_application_error( -20985, strErr );
          PROGRESS.END_OF_PROCESS( p_nProcessId );
          return;
      end;
    ELSE
      strSQL := 'DELETE FROM ' || strHHTblName;
      begin
        execute immediate strSQL;
      exception
        when OTHERS then
          strErr := 'Error clearing ' || strHHTblName || ' table';
          raise_application_error( -20984, strErr );
          PROGRESS.END_OF_PROCESS( p_nProcessId );
          return;
      end;
    end if;

    PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nRetval );
    COMMIT;
    IF ( nRetval = 0 ) then
      PROGRESS.END_OF_PROCESS( p_nProcessId );
      RETURN;
    end if;

    strsql :=
            'INSERT INTO '
         || strhhtblname
         || '( BOOK_NO, WALK_NO, SEQNCE_NO, POS_STATUS, BILL_CUSTKEY,
      TOWNSHIP_NO, PLOT_NO, PROP_NO, PROP_REF, CONN_NO, SERV_CATG, UA_ADRESS1, UA_ADRESS2, UA_ADRESS3,
      UA_ADRESS4, C_TYPE, OLD_KEY, CON_STATUS, METER_TYPE, METER_REF, SERIAL_NO, MAKE, NO_DIALS, CONV_FCTR,
      "SIZE", CLOCK_OVER, FRCD_READ, DIALS_CHNG, NEW_CONNCT, DESCREPANCY, DESCR_MSG, "ACTION", ACTION_MSG,
      PROPERTY_VACATED, OP_STATUS, LOC_CODE, LOC_POS, STAMP_DATE, STAMP_USER )
    ( SELECT CASE NVL( CMB.BOOK_NO, N'''' ) WHEN N'''' THEN N''U'' ELSE
      CASE MB.BILLGROUP WHEN N'''
         || p_strbillgroup
         || ''' THEN CMB.BOOK_NO ELSE N''U'' END END,
      CASE NVL( CMB.BOOK_NO, N'''' ) WHEN N'''' THEN 1 ELSE CASE MB.BILLGROUP WHEN N'''
         || p_strbillgroup
         || '''
      THEN CMB.WALK_NO ELSE 1 END END,
      CASE NVL( CMB.BOOK_NO, N'''' ) WHEN N'''' THEN ROWNUM ELSE CASE MB.BILLGROUP
      WHEN N'''
         || p_strbillgroup
         || ''' THEN CMB.F_SEQUNCE ELSE ROWNUM END END,
      CMB.CON_STATUS, PRP.BILL_CUSTKEY, PRP.TOWNSHIP_NO, PRP.PLOT_NO, PRP.PROP_NO, PRP.PROP_REF, CMB.CONN_NO,
      CMB.SERV_CATG, PRP.UA_ADRESS1, PRP.UA_ADRESS2, PRP.UA_ADRESS3, PRP.UA_ADRESS4, PRP.C_TYPE, PRP.OLD_KEY,
      CMB.CON_STATUS, MTR.METER_TYPE, MTR.METER_REF, MTR.SERIAL_NO, MTR.MAKE, MTR.NO_DIALS, MTR.CONV_FCTR,
      MTR."SIZE", 0, 0, 0, 0, 2, ''no reading [other]'', 1, ''read'', PRP.IS_VACATED, MTR.OP_STATUS,
      ML.CODE, ML.DESCRIBE, SYSDATE, '''
         || p_strcurrentuser
         || '''';

      /*
      strsql2 :=
            ' FROM CONN_DTL CMB
        INNER JOIN METR_DTL MTR ON CMB.METER_TYPE = MTR.METER_TYPE AND CMB.METER_REF = MTR.METER_REF
        INNER JOIN PROP_DTL PRP ON CMB.PROP_REF = PRP.PROP_REF
        INNER JOIN CUST_DTL CST ON PRP.BILL_CUSTKEY = CST.CUSTKEY
        INNER JOIN METER_BOOKS MB ON CMB.BOOK_NO = MB.CODE
        LEFT  JOIN LU_METERLOCATIONS ML ON ML.CODE = CMB.LOC_CODE
      WHERE CST.BILLGROUP = '''
         || p_strbillgroup
         || ''' )';

      */

     strsql2 :=
            ' FROM CONN_DTL CMB,METR_DTL MTR ,PROP_DTL PRP,CUST_DTL CST,METER_BOOKS MB ,LU_METERLOCATIONS ML
        where CMB.METER_TYPE = MTR.METER_TYPE AND CMB.METER_REF = MTR.METER_REF
        and  CMB.PROP_REF = PRP.PROP_REF
        and  PRP.BILL_CUSTKEY = CST.CUSTKEY
        and  CMB.BOOK_NO = MB.CODE
        and   ML.CODE(+) = CMB.LOC_CODE
        and   CST.BILLGROUP = '''
             || p_strbillgroup
             || ''' )';

    begin
      execute immediate strSQL || strSQL2;
    exception
      when OTHERS then
        strErr := 'Error inserting new records into ' || strHHTblName || ' table';
        raise_application_error( -20983, strErr );
        PROGRESS.END_OF_PROCESS( p_nProcessId );
        RETURN;
    end;

    PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nRetval );
    COMMIT;
    IF ( nRetval = 0 ) then
      PROGRESS.END_OF_PROCESS( p_nProcessId );
      RETURN;
    end if;

    strSQL := 'UPDATE ' || strHHTblName || ' Set TENT_NAME = LTRIM( RTRIM (
          (SELECT SubStr(SURNAME || '' '' || TITLE,1,40)  FROM CUST_DTL WHERE CUSTKEY = ' ||
          strHHTblName || '.BILL_CUSTKEY)' || ' ) )';
    begin
      execute immediate strSQL;
    exception
      when OTHERS then
        strErr := 'Error updating owner''s and billed customer''s name in ' || strHHTblName || ' table';
        raise_application_error( -20982, strErr );
        rollback;
        PROGRESS.END_OF_PROCESS( p_nProcessId );
        RETURN;
    end;

    PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nRetval );
    COMMIT;
    IF ( nRetval = 0 ) then
      rollback;
      PROGRESS.END_OF_PROCESS( p_nProcessId );
      RETURN;
    end if;

  /*** Update of previous reading date ***/
    delete from TMP_METR_RDGS;


   /********************************************************************************************************************************/
   /********************************************************************************************************************************/

   /* ---------------------------------------------------------------------------
      Extract historical information for 6 latest readings
      --------------------------------------------------------------------------- */
    for nCnt in 1..6
    loop

       PROGRESS.START_NEW_PHASE(p_nProcessId, 0, nContinue); commit;  /* Step 5-10 */
       if ( nContinue != 1 ) then return; end if;


       strSQL:= 'update ' || strHHTblName ||
                ' set (PR_RDGN' || CAST( nCnt AS varchar2 ) || ',' ||
                '      PR_DATE' || CAST( nCnt AS varchar2 ) || ',' ||
                '      PR_READ' || CAST( nCnt AS varchar2 ) || ',' ||
                '      PR_CONS' || CAST( nCnt AS varchar2 ) || ',' ||
                '      PR_FLOW' || CAST( nCnt AS varchar2 ) || ')' ||
                '       = ( select READING_NO, CUR_DATE, B_READING, CONSUMP, FLOW ' ||
                '        from vwMETR_RDG r ' ||
                '        where r.METER_TYPE = ' || strHHTblName || '.METER_TYPE and r.METER_REF = ' ||  strHHTblName || '.METER_REF ' ||
                '          and r.READING_NO <= coalesce( ( select min(READING_NO) ' ||
                '                                          from vwMETR_RDG r1 ' ||
                '                                          where r1.METER_TYPE = r.METER_TYPE and r1.METER_REF = r.METER_REF ' ||
                '                                            and r1.READ_REASN = 1 ' ||
                '                                            and coalesce(r1.IS_CANCELLED, 0) = 0 ), 0 ) ' ||
                '          and r.READING_NO = ( select min(READING_NO) ' ||
                '                                 from vwMETR_RDG r1 ' ||
                '                                where r1.METER_TYPE = r.METER_TYPE and r1.METER_REF = r.METER_REF ' ||
                '                                  and coalesce(r1.IS_CANCELLED, 0) = 0  ';


       if nCnt = 1 then
          strSQL:= strSQL || ' ))';
       else
          strSQL:= strSQL || ' and r1.READING_NO > ' || strHHTblName || '.PR_RDGN' ||  CAST( nCnt-1 AS varchar2 ) || '))';
       end if;


       begin
         execute immediate strSQL;
        exception
         when others then
          PROGRESS.PROCESS_FAIL(p_nProcessId, 8); commit;
          strErr := 'Error updating historical reading ' || CAST( nCnt AS varchar2 ) || ' ';
          raise_application_error( -20980, strErr );
          return;
       end;


    end loop;

    /* ---------------------------------------------------------------------------
       Update Average Consumption
       --------------------------------------------------------------------------- */
    PROGRESS.START_NEW_PHASE(p_nProcessId, 0, nContinue); commit;  /* Step 11 */
    if ( nContinue != 1 ) then return; end if;

    /* Calculate average consumption for previous readings */
    strSQL:= '
       update ' || strHHTblName || '
       set PR_AVR_CNS =
          case trunc(PR_DATE1) - trunc( coalesce( PR_DATE6, coalesce( PR_DATE5, coalesce( PR_DATE4,
                                        coalesce( PR_DATE3, coalesce( PR_DATE2, PR_DATE1 ) ) ) ) ) )
            when 0 then NULL
            else ( PR_READ1 - coalesce( PR_READ6, coalesce( PR_READ5, coalesce( PR_READ4,
                              coalesce( PR_READ3, coalesce( PR_READ2, PR_READ1 ) ) ) ) ) ) /
                   cast( trunc(PR_DATE1) - trunc( coalesce(PR_DATE6, coalesce(PR_DATE5, coalesce(PR_DATE4,
                                                  coalesce(PR_DATE3, coalesce(PR_DATE2, PR_DATE1 ) ) ) ) ) ) as number)
          end';

    begin
      execute immediate strSql;
    exception
      when others then
        PROGRESS.PROCESS_FAIL(p_nProcessId, 8);
        raise_application_error( -20979, 'Error updating the average consumption ' );
        return;
    end;

   /********************************************************************************************************************************/
   /********************************************************************************************************************************/



    /*** Initialise WALKROUTES_HISTORY Table ***/
    PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nRetval );
    COMMIT;
    IF ( nRetval = 0 ) then
      rollback;
      PROGRESS.END_OF_PROCESS( p_nProcessId );
      RETURN;
    end if;

  delete from WALKROUTES_HISTORY a
   where exists (select 1 from METER_BOOKS b
                  where a.BOOK_NO = b.CODE
              and b.BILLGROUP = p_strBillGroup);

    select count(1) into nTotRecords
   from METER_BOOKS
  where BILLGROUP = p_strBillGroup ;

    nTotRecords := nvl(nTotRecords,0);
    PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
    nCount := 0;
    nIncrement := floor(nTotRecords / 50);
    if nIncrement < 1 then
      nIncrement := 1;
    end if;

    nCnt := 1;

  for WALKROUTE_CURSOR in
   (select CODE, NO_WALKS, HANDHELD_ID
      from METER_BOOKS
     where BILLGROUP = p_strBillGroup)
  loop

    /* Display Progress */
      if nCount = nIncrement then
        PROGRESS.START_NEW_RECORD (p_nProcessId, WALKROUTE_CURSOR.CODE, nIncrement);
        commit;
      if ( nContinue != 1 ) then return; end if;
        nCount := 0;
      end if;
      nCount := nCount + 1;

    while  nCnt <=  WALKROUTE_CURSOR.NO_WALKS
    loop

    insert into WALKROUTES_HISTORY(BOOK_NO,
                                   WALK_NO,
                     HHUNIT_ID,
                     CYCLE_NO,
                                   PROCESS_ST,
                     HH_DATE,
                     POST_DATE,
                     STAMP_USER)
    values  (WALKROUTE_CURSOR.CODE,
             nCnt,
         WALKROUTE_CURSOR.HANDHELD_ID,
         0, /*p_nCurrBillCycle,*/
             0,
         to_date('30/12/1899','DD/MM/YYYY'),
         to_date('30/12/1899','DD/MM/YYYY'),
         p_strCurrentUser
         );

        nCnt := nCnt + 1;

    end loop;

                nCnt := 1;

  end loop;


    PROGRESS.END_OF_PROCESS( p_nProcessId );
    COMMIT;
  END CREATEHHDATA;

   PROCEDURE produceinstalments (
      p_strbillgroup      IN   NVARCHAR2,
      p_nbillcycleid      IN   NUMBER,
      p_dtbillcycledate   IN   DATE,
      p_nprocessid        IN   NUMBER,
      p_strCustkey        IN   NVARCHAR2)
   AS
      ncontinue         NUMBER (10);
      nlasttransno      NUMBER (10);
      strcustkey        NVARCHAR2 (15);
   BEGIN
      progress.start_of_process (p_nprocessid, 5); /* Insert all effective date based installments into temporary table */

      BEGIN
           DELETE FROM current_agreem_installments ;----sHOW
           COMMIT ;

         INSERT INTO current_agreem_installments
            SELECT ai.custkey, ai.agrm_no, installment_no inst_no,
                   ca.agrm_schedule, trans_no, amount, charge_date
              FROM agreement_installments ai, cust_agreem ca, cust_dtl cst
             WHERE cst.billgroup = p_strbillgroup
               AND ai.agrm_no = ca.agrm_no
               AND ai.custkey = ca.custkey
               AND cst.custkey = ca.custkey
               AND ca.agrm_schedule = 0
               AND ai.charge_date <= p_dtbillcycledate
               AND ca.agrm_type IN (0, 1, 2)
               AND ca.agrm_status = 0
               AND ai.trans_no > 0
               and cst.CUSTKEY = case when length(p_strCustkey) > 0 then p_strCustkey else cst.CUSTKEY end /*2017-11*/;

      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error
               (-20979,
                'Error inserting effective date based installments into temporary table'
               );
            progress.end_of_process (p_nprocessid);
            RETURN;
      END;

      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF ncontinue = 0
      THEN
         ROLLBACK;
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF; /* Insert all billing cycle based installments into temporary table */

      BEGIN
         INSERT INTO current_agreem_installments
            SELECT ai.custkey, ai.agrm_no, ai.installment_no inst_no,
                   ca.agrm_schedule, trans_no, amount, p_dtbillcycledate
              FROM agreement_installments ai,
                   cust_agreem ca,
                   cust_dtl cst,
                   (SELECT   agrm_no, custkey,
                             MIN (installment_no) installment_no
                    FROM agreement_installments
                    WHERE trans_no > 0
                    GROUP BY agrm_no, custkey) ai2
             WHERE cst.billgroup = p_strbillgroup
               AND ai.agrm_no = ca.agrm_no
               AND ai.custkey = ca.custkey
               AND cst.custkey = ca.custkey
               AND ca.agrm_schedule = 1
               AND ca.agrm_type IN (0, 1, 2)
               AND ca.agrm_status = 0
               AND ai.trans_no > 0
               AND ai.agrm_no = ai2.agrm_no
               AND ai.custkey = ai2.custkey
               AND ai.installment_no = NVL (ai2.installment_no, -1)
               and cst.CUSTKEY = case when length(p_strCustkey) > 0 then p_strCustkey else cst.CUSTKEY end /*2017-11*/;
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error
               (-20978,
                'Error inserting billing cycle based installments into temporary table'
               );
            ROLLBACK;
            progress.end_of_process (p_nprocessid);
            RETURN;
      END;


      /* 2017-11 Remove already produced */
      delete from current_agreem_installments tmp
       where exists (select 1 from F_TRANS ft
                      where tmp.CUSTKEY = ft.CUSTKEY
                        and ft.BILL_CYCLE_ID =  p_nbillcycleid
                        and ft.BILL_CYCLE_STEP = 4);


      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF ncontinue = 0
      THEN
         ROLLBACK;
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF; /* Calculate transaction numbers for all installments in temporary table */

      strcustkey := '';

      FOR curagr IN (SELECT   custkey, agrm_no, inst_no
                         FROM current_agreem_installments
                     ORDER BY custkey)
      LOOP
         IF strcustkey <> curagr.custkey OR strcustkey IS NULL
         THEN
            strcustkey := curagr.custkey;

            SELECT NVL (MIN (trans_no), 0)
              INTO nlasttransno
              FROM vwf_trans
             WHERE custkey = strcustkey;
         END IF;

         BEGIN
            UPDATE current_agreem_installments
               SET trans_no = nlasttransno - 1
             WHERE custkey = strcustkey
               AND agrm_no = curagr.agrm_no
               AND inst_no = curagr.inst_no;
         EXCEPTION
            WHEN OTHERS
            THEN
               raise_application_error
                      (-20977,
                       'Error selecting transaction numbers for installments'
                      );
               ROLLBACK;
               progress.end_of_process (p_nprocessid);
               RETURN;
         END;

         nlasttransno := nlasttransno - 1;
      END LOOP;

      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF ncontinue = 0
      THEN
         ROLLBACK;
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF; /* Create installments transactions */

      BEGIN
         INSERT INTO f_trans
                     (trans_no, trns_type, trns_stype, custkey, billgroup,
                      amount, effect_dte, discount, documnt_no, memo_fld,
                      statm_no, cancelled, reversed, bill_cycle_id,
                      bill_cycle_step, rev_cycle_id)
            SELECT ai.trans_no, ca.trns_type, ca.trns_stype, ca.custkey,
                   p_strbillgroup, ai.amount, ai.charge_date, 0.0,
                   ca.agrm_no, ca.agrm_ref || ' - ????? ??? ' || ai.inst_no,
                   0, 0, 0, p_nbillcycleid, 4, 0
              FROM cust_agreem ca, current_agreem_installments ai
             WHERE ca.custkey = ai.custkey AND ca.agrm_no = ai.agrm_no;
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error
                                  (-20977,
                                   'Error creating installments transactions'
                                  );
            ROLLBACK;
            progress.end_of_process (p_nprocessid);
            RETURN;
      END;

      /* 2019-11 */
      /* create installments penalty transactions */
      BEGIN

        FOR curagr IN (  SELECT custkey, agrm_no, inst_no
                           FROM CURRENT_AGREEM_INSTALLMENTS ai
                          WHERE exists (select 1 from CUST_AGREEM ca
                                         where ca.custkey = ai.custkey
                                           and ca.agrm_no = ai.agrm_no
                                           and ca.PENALTY_TYPE in (1,2)) )
        LOOP

         INSERT INTO f_trans (trans_no,
                              trns_type,
                              trns_stype,
                              custkey,
                              billgroup,
                              amount,
                              effect_dte,
                              discount,
                              documnt_no,
                              memo_fld,
                              statm_no,
                              cancelled,
                              reversed,
                              bill_cycle_id,
                              bill_cycle_step,
                              rev_cycle_id)
            SELECT (SELECT NVL (MIN (trans_no), 0) - 1 FROM vwf_trans ft WHERE ft.custkey = ai.custkey),
                   50,
                   17,
                   ca.custkey,
                   p_strbillgroup,
                   case
                     when ca.PENALTY_TYPE = 1 then
                       round(ai.amount * (ca.PENALTY_VALUE / 100),2)
                     when ca.PENALTY_TYPE = 2 then
                       round( (ca.PENALTY_VALUE / ca.NO_INSTALMENTS) ,2)
                   end,
                   ai.charge_date,
                   0.0,
                   ca.agrm_no,
                   ca.agrm_ref || ' - ????? ??? ' || ai.inst_no,
                   0,
                   0,
                   0,
                   p_nbillcycleid,
                   4,
                   0
              FROM cust_agreem ca, current_agreem_installments ai
             WHERE ca.custkey = ai.custkey
               AND ca.agrm_no = ai.agrm_no
               and ai.custkey = curagr.custkey
               and ai.agrm_no = curagr.agrm_no
               and ai.inst_no = curagr.inst_no;

       END LOOP;

     EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error (
               -20977,
               'Error creating installments penalty transactions');
            ROLLBACK;
            progress.end_of_process (p_nprocessid);
            RETURN;
     END;


      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF ncontinue = 0
      THEN
         ROLLBACK;
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF; /* Update transaction numbers in agreement installments */

      BEGIN
         UPDATE agreement_installments ai
            SET (ai.charge_date, ai.trans_no) =
                   (SELECT cai.charge_date, cai.trans_no
                      FROM current_agreem_installments cai
                     WHERE cai.custkey = ai.custkey
                       AND cai.agrm_no = ai.agrm_no
                       AND cai.inst_no = ai.installment_no)
          WHERE EXISTS (
                   SELECT 1
                     FROM current_agreem_installments cai2
                    WHERE cai2.custkey = ai.custkey
                      AND cai2.agrm_no = ai.agrm_no
                      AND cai2.inst_no = ai.installment_no);
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error (-20976,
                                     'Error updating agreement installments'
                                    );
            ROLLBACK;
            progress.end_of_process (p_nprocessid);
            RETURN;
      END;

      progress.start_new_phase (p_nprocessid, 0, ncontinue);
      COMMIT;

      IF ncontinue = 0
      THEN
         ROLLBACK;
         progress.end_of_process (p_nprocessid);
         RETURN;
      END IF; /* Update agreements as closed if the last installment has been just processed */

      BEGIN
         UPDATE cust_agreem
            SET agrm_status = 1, /* Agreement closed */
                completion_date = SYSDATE
          WHERE EXISTS (
                   SELECT 1
                     FROM current_agreem_installments cai
                    WHERE cai.agrm_no = cust_agreem.agrm_no
                      AND cai.custkey = cust_agreem.custkey
                      AND cai.inst_no = cust_agreem.no_instalments);
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error (-20975,
                                     'Error updating customer agreements'
                                    );
            ROLLBACK;
            progress.end_of_process (p_nprocessid);
            RETURN;
      END;

      progress.end_of_process (p_nprocessid);
      COMMIT;
   END produceinstalments;

   PROCEDURE POSTROUNDINGDIFFERENCE
  ( p_nAmount        NUMBER,
    p_strCustKey     NVARCHAR2,
    p_strBillGroup   NVARCHAR2,
    p_nStatmNo       NUMBER,
    p_dtBillingDate  DATE,
    p_dtEffectDte    DATE,
    p_nBillCycleId   NUMBER,
    p_nTrnsType      NUMBER,
    p_nTrnsSType     NUMBER,
    p_strCurrentUser NVARCHAR2,
  p_memofld        NVARCHAR2,
    p_propref        NVARCHAR2,
    p_penaltyamt     NUMBER,
    p_nNewTransNo    out NUMBER
  )
  AS
    nCycleStep       NUMBER(10);
    nSrvCategory     NUMBER(10);
    strItemCode      NVARCHAR2(10);
    strItemSCode     NVARCHAR2(10);
    strVatCategory   NVARCHAR2(10);
    strVatItemCode   NVARCHAR2(10);
    strVatItemSCode  NVARCHAR2(10);
  begin
    nCycleStep := 6;  /*kStatements*/

    SELECT NVL(GL_SRVCTG,0),
         GL_ITMCOD,
       GL_SITMCOD,
       NVL(VAT_SRVCTG, 0),
       VAT_ITMCOD,
       VAT_SITMCOD
      into nSrvCategory, strItemCode, strItemSCode, strVatCategory, strVatItemCode, strVatItemSCode
      FROM TRANSACTION_TYPES
     WHERE TRNS_CODE  = p_nTrnsType
       AND TRNS_SCODE = p_nTrnsSType;

    SELECT NVL(MIN(TRANS_NO),0) INTO p_nNewTransNo
      FROM VWF_TRANS
     WHERE CUSTKEY = p_strCustKey;

    p_nNewTransNo := p_nNewTransNo - 1;

    INSERT INTO F_TRANS ( BILLGROUP, CUSTKEY, TRANS_NO, STATM_NO,
                          BILNG_DATE, EFFECT_DTE,
                          REVERSED, CANCELLED,
                          TRNS_TYPE, TRNS_STYPE, PROP_REF,
                    AMOUNT, BILL_CYCLE_ID, BILL_CYCLE_STEP, REV_CYCLE_ID,
                          SERV_CATG, ITEM_CODE, SITEM_CODE,
                          VAT_SRVCTG, VAT_ITM_CD, VAT_SITMCD,
                          MEMO_FLD,
                          BAL_CFWD,
                          STAMP_DATE, STAMP_USER )
    VALUES ( p_strBillGroup, p_strCustKey, p_nNewTransNo, p_nStatmNo,
             p_dtBillingDate, p_dtEffectDte,
             0,0,
             p_nTrnsType, p_nTrnsSType, p_propref,
            p_nAmount, p_nBillCycleId, nCycleStep, 0,
             nSrvCategory, strItemCode, strItemSCode,
             strVatCategory, strVatItemCode, strVatItemSCode,
             p_memofld,
             p_penaltyamt, SYSDATE, p_strCurrentUser );

  commit;
  end POSTROUNDINGDIFFERENCE;

   PROCEDURE allocatecharges (
      p_strcustkey       IN   NVARCHAR2,
      p_nbillcycleid     IN   NUMBER,
      p_npaymenttrnsno   IN   NUMBER,
      p_npaymentamount   IN   NUMBER,
      p_strcurrentuser   IN   NVARCHAR2
   )
   AS
      nnewindex   NUMBER (10);
      nallocamt   NUMBER (19, 4);
      npayment    NUMBER (19, 4);
      nallocpay   NUMBER (19, 4);
   BEGIN
      npayment := p_npaymentamount;
      nallocamt := 0;
      nallocpay := 0;

      SELECT COALESCE (MAX (item_index), 1)
        INTO nnewindex
        FROM allocation_payments;

      nnewindex := nnewindex + 1;

      FOR trans_cursor IN
         (SELECT trans_no, amount
                           - COALESCE (allocated_amount, 0) unallocated
            FROM vwf_trans
           WHERE custkey = p_strcustkey
             AND amount > 0.0
             AND COALESCE (cancelled, 0) <> 1
             AND COALESCE (allocated_amount, 0.0) < amount
             AND bill_cycle_id = p_nbillcycleid
             AND amount >
                    COALESCE ((SELECT SUM (ac.charge_amount)
                                 FROM allocation_payments ap,
                                      allocation_charges ac
                                WHERE ap.custkey = p_strcustkey
                                  AND ap.custkey = ac.custkey
                                  AND ap.item_index = ac.item_index
                                  AND ap.allocation_status = 0
                                  AND ac.charge_trans_no = trans_no),
                              0
                             ))
      LOOP
         EXIT WHEN (npayment <= 0);

         IF (npayment - trans_cursor.unallocated > 0)
         THEN
            nallocamt := trans_cursor.unallocated;
            npayment := npayment - trans_cursor.unallocated;
            nallocpay := nallocpay + trans_cursor.unallocated;
         ELSE
            nallocamt := npayment;
            nallocpay := nallocpay + npayment;
            npayment := npayment - trans_cursor.unallocated;
         END IF;

         INSERT INTO allocation_charges
                     (item_index, custkey, charge_trans_no,
                      charge_amount, stamp_date, stamp_user
                     )
              VALUES (nnewindex, p_strcustkey, trans_cursor.trans_no,
                      nallocamt, SYSDATE, p_strcurrentuser
                     );

         UPDATE f_trans
            SET allocated_amount = COALESCE (allocated_amount, 0) + nallocamt
          WHERE custkey = p_strcustkey AND trans_no = trans_cursor.trans_no;
      END LOOP;

      IF (p_npaymentamount > 0)
      THEN
         INSERT INTO allocation_payments
                     (item_index, custkey, settlement_type,
                      allocation_status, settlement_doc_ref,
                      payment_trans_no, payment_amount, full_payment,
                      stamp_date, stamp_user
                     )
              VALUES (nnewindex, p_strcustkey, 5,
                      1, p_npaymenttrnsno,
                      p_npaymenttrnsno, nallocpay, p_npaymentamount,
                      SYSDATE, p_strcurrentuser
                     );

         UPDATE f_trans
            SET allocated_amount = COALESCE (allocated_amount, 0) + nallocpay
          WHERE custkey = p_strcustkey AND trans_no = p_npaymenttrnsno;
      END IF;

      COMMIT;
   END allocatecharges;

/* ===========================================================================
   ===========================================================================
   ====  Billing Step 6 - Generate Statements  ===============================
   ===========================================================================
   =========================================================================== */


  procedure GENERATESTATEMENTS( p_strBillGroup    nvarchar2,
                                p_dtBillCycleDate date,
                                p_nBillCycleId    number,
                                p_nStation        number,
                                p_strCurrentUser  nvarchar2,
                                p_nProcessId      number,
                                p_strCustkey      nvarchar2)
  AS

  /***************************************************************/
  /* PHASES                                                      */
  /* ------                                                      */
  /* 1. CREATE TEMPORARY TABLE FOR STATEMENTS TO BE GENERATED    */
  /* 2. CALCULATION OF PENATIES                                  */
  /* 3. CALCULATE AND PERFORM ROUNDING ON CLOSING BALANCE        */
  /* 4. GROUP ACCOUNTS PROCESSING                                */
  /* 5. GROUPING OF TRANSACTIONS                                 */
  /* 6. INSTALLMENTS CALCULATION                                 */
  /* 7. RECEIPTING CHARGES CALCULATION                           */
  /* 8. AGEIING ANALYSIS CALCULATION                             */
  /* 9. PAY-BY-DATE CALCULATION                                  */
  /* 10.CALCULATION OF NEXT PAYMENT NO                           */
  /* 11.CREATION OF STATEMENTS                                   */
  /***************************************************************/

  /****************************************************************************************/
  /* Constant Declarations                                                                */
  /****************************************************************************************/

  kMaxTrnsCnt     number(10);
  kStatmNoLen     number(10);

  /* Installments settings */
  kNoInstallments number(10);
  kInstFixedAmt   number(10);
  kInstPercent    number(10);

  /* Transaction types */
  kReceipting     number(10);
  kJournals       number(10);
  kCreditNote     number(10);
  kDebitNote      number(10);

  /* Transaction sub-types */
  kReceiptingCons number(10);
  kDebitPenalty   number(10);
  kRoundingDiff   number(10);
  kRcptCharge1    number(10);
  kRcptCharge2    number(10);
  kRcptCharge3    number(10);
  kRcptCharge4    number(10);
  kRcptCharge5    number(10);
  kJournalsDebit  number(10);
  kJournalsCredit number(10);

  kRcptChargesCnt number(10); /* Number of receipting charges */

  /****************************************************************************************/
  /* Global Variable Declarations                                                         */
  /****************************************************************************************/

  strSQL      varchar2(4000);
  nContinue   number(10);
  nTotRecords number(10);
  nCount      number(10);
  nIncrement  number(10);
  nNewStatmNo number(10);

  /* Rounding Related Setting Variables */
  nIssueRoundingDiff number(10);
  nRoundDown         number(10);
  nRoundUp           number(10);
  nRoundNearest      number(10);
  nRoundTo           number(10);
  nNoOfDigits        number(10);
  nRoundingMethod    number(10);

  /* 2.CALCULATION OF PENATIES */
  nPrevCycleId     number(10);
  strCustKey       nvarchar2(15);
  strPropRef       nvarchar2(20);
  nAmount          number(19,4);
  nPenalty         number(19,4);
  nAmountDiff      number(19,4);
  nPenaltyCharge   number(19,4);
  nNewTransNo      number(10);
  ndtPrevBilngDte  date;

  /* Penalty variables and settings  */
  nPenaltyOption   number(10);
  nOpenItems       number(10);

  /* 3.CALCULATE AND PERFORM ROUNDING ON CLOSING BALANCE */
  nBalance      number(19,4);
  nBalanceDiff  number(19,4);
  nStatmNo      number(10);
  nTrnsType     number(10);

  /* 4.GROUP ACCOUNTS PROCESSING */
  strGrpBilGrp  nvarchar2(5);
  dtGrpEffect   date;

  /* 6. INSTALLMENTS CALCULATION */
  nStatmArrears      number(19,4);
  nStatmArrearsDiff  number(19,4);
  nInstSetting       number(10);
  nNoNegBalance      number(10);

  /* 7. RECEIPTING CHARGES CALCULATION */
  bitUseRcptChg number(1);
  nUseRcptChg   number(10);
  nRcptChrgCnt  number(10);
  kRcptCharge   number(10);

  nRctChrg1     number(19,4);
  nRctChrg2     number(19,4);
  nRctChrg3     number(19,4);
  nRctChrg4     number(19,4);
  nRctChrg5     number(19,4);
  Rounddiff     number(19,4);

  /* 10. CALCULATION OF NEXT PAYMENT NO */
  nNextPaymentNo  number(10);
  strPaymentNoHdr nvarchar2(4); /*Two digits for the year and two for the station number */

  /* 11. CREATION OF STATEMENTS */
  nRowsUpdated    number(10);
  nSubStatmNo     number(10);

  strTmpCustKey  nvarchar2(15);
  nTrans         number(10);

  begin

  /****************************************/
  /* Initialization of Constant Variables */
  /****************************************/

  kMaxTrnsCnt     := 10;   /* Maximum number of transactions per sub-statement */
  kStatmNoLen     := 8;    /* Two digits for the year, two for the station and 8 for the number */

  kNoInstallments := 0;
  kInstFixedAmt   := 1;
  kInstPercent    := 2;

  kReceipting     := 20;
  kJournals       := 30;
  kCreditNote     := 40;
  kDebitNote      := 50;

  kReceiptingCons := 2;
  kDebitPenalty   := 12;
  kRoundingDiff   := 13;
  kRcptCharge1    := 51;
  kRcptCharge2    := 52;
  kRcptCharge3    := 53;
  kRcptCharge4    := 54;
  kRcptCharge5    := 55;
  kJournalsDebit  := 1;
  kJournalsCredit := 2;

  kRcptChargesCnt := 5;

  /********************************************/
  /***  All related rounding settings       ***/
  /********************************************/

  select nvl(KEYWORD_VALUE,0) into nIssueRoundingDiff
    from SETTINGS
   where KEYWORD = 'GEN_SEPERATE_TRANS_FOR_DIFF';

  select nvl(KEYWORD_VALUE,0) into nRoundDown
    from SETTINGS
   where KEYWORD = 'GEN_ROUND_MTHD_TRUNC';

  select nvl(KEYWORD_VALUE,0) into nRoundUp
   from SETTINGS
  where KEYWORD = 'GEN_ROUND_MTHD_UP';

  select nvl(KEYWORD_VALUE,0) into nRoundNearest
    from SETTINGS
   where KEYWORD = 'GEN_ROUND_MTHD_NEAREST';

  /* select nvl(KEYWORD_VALUE,1) into nRoundTo
     from SETTINGS
     where KEYWORD = 'GEN_CURRENCY_ROUNDTO';  */

   nRoundTo := 500 ;

  select nvl(KEYWORD_VALUE,2) into nNoOfDigits
    from SETTINGS
   where KEYWORD = 'GEN_CURRENCY_DIGITS';

  if (nRoundNearest = 1) then
      nRoundingMethod := 1;
  elsif (nRoundDown = 1) then
      nRoundingMethod := 3;
  elsif (nRoundUp = 1) then
      nRoundingMethod := 2;
  else
      nRoundingMethod := 1;
  end if;

  PROGRESS.START_OF_PROCESS( p_nProcessId, 11 );

  commit;

  /* Tables should be already empty so DELETE should not be expensive */


  /*******************************************************************************************************/
  /***  1.CREATE TEMPORARY TABLE FOR STATEMENTS TO BE GENERATED                                        ***/
  /*******************************************************************************************************/

  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  delete from TMP_CUST_STATM;

  insert into TMP_CUST_STATM
  (
  PAYMENT_ID,
  CUSTKEY,
  NEW_STATM_NO,
  TRANS_CNT,
  CONSUMER_TYPE,
  OP_BLNCE,
  CL_BLNCE,
  CUR_CHARGES,
  ADJUSTMENTS,
  CUR_PAYMENTS,
  CL_BLNCE_PEN,
  AGE_CUR,
  AGE_30,
  AGE_60,
  AGE_90,
  AGE_120,
  AGE_150,
  AGE_180,
  AGE_1YR,
  AGE_2YR,
  CL_BLNCE_ROUNDED,
  INSTALLMENT,
  PAY_BY,
  RCPT_CHARGE1,
  RCPT_CHARGE2,
  RCPT_CHARGE3,
  RCPT_CHARGE4,
  RCPT_CHARGE5,
  PAYMENT_NO
  )
  select rownum,
         CUSTKEY,
         0,
         0,
         CONSUMER_TYPE,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         0,
         NULL,
         0,
         0,
         0,
         0,
         0,
         NULL
    from CUST_DTL
   where BILLGROUP = p_strBillGroup
     and coalesce(ACC_TYPE, 0) <> 2
     and CUSTKEY = case when length(p_strCustkey) > 0 then p_strCustkey else CUSTKEY end /*2017-11*/;

   /* 2017-11 Remove already produced */
   delete from TMP_CUST_STATM tmp
    where exists (select 1 from F_TRANS ft
                   where tmp.CUSTKEY = ft.CUSTKEY
                     and ft.BILL_CYCLE_ID =  p_nbillcycleid
                     and ft.BILL_CYCLE_STEP = 6);

  commit;

  update TMP_CUST_STATM
     set NEW_STATM_NO = coalesce( (select min(STATM_NO)
                                     from VWF_STATM FS
                                    where FS.CUSTKEY = TMP_CUST_STATM.CUSTKEY
                                   ), 0 ) - 1;

  commit;

  update TMP_CUST_STATM t
     set (OP_BLNCE, CL_BLNCE)  = (select fs.CL_BLNCE, fs.CL_BLNCE
                                    from vwF_STATM fs
                                   where fs.CUSTKEY   = t.CUSTKEY
                                     and fs.STATM_NO  = t.NEW_STATM_NO + 1
                                     and fs.SUBSTM_NO = 1);

  commit;

  update TMP_CUST_STATM
     set OP_BLNCE = 0
   where OP_BLNCE is null;

  update TMP_CUST_STATM
     set CL_BLNCE = 0
   where CL_BLNCE is null;

  commit;


  /* Mark the transactions in F_TRANS for these customers which will be billed */

  select count(1) into nTotRecords from TMP_CUST_STATM;

  nTotRecords := nvl(nTotRecords,0);
  PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
  nCount := 0;
  nIncrement := floor(nTotRecords / 50);
  if nIncrement < 1 then
    nIncrement := 1;
  end if;

  for UPDATE_CURSOR in (select CUSTKEY, NEW_STATM_NO  from TMP_CUST_STATM order by CUSTKEY)
  loop

    /* Display Progress */
    if nCount = nIncrement then
       PROGRESS.START_NEW_RECORD (p_nProcessId, update_cursor.custkey , nIncrement);
       commit;
     nCount := 0;
    end if;
    nCount := nCount + 1;

    update F_TRANS ft
       set STATM_NO   = UPDATE_CURSOR.NEW_STATM_NO,
           BILNG_DATE = p_dtBillCycleDate,
       BILLGROUP  = p_strBillGroup
     where ft.CUSTKEY = update_cursor.custkey
       and coalesce(ft.STATM_NO,0) = 0
       and ft.EFFECT_DTE <= p_dtBillCycleDate
       and coalesce(ft.CANCELLED,0) = 0;

  end loop;

  commit;

  /**********************************************************/
  /***  2.CALCULATION OF PENATIES                           */
  /**********************************************************/

  select nvl(KEYWORD_VALUE,0) into nOpenItems
    from settings
   where keyword = 'OPEN_ITEM_TRANSACTIONS';

  select nvl(KEYWORD_VALUE,0) into nPenaltyOption
    from settings
   where KEYWORD = 'BIL_PENALTIES_Option';


  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  /* @nPenaltyOption: 1 = Unpaid since specific biling, 2 = Fixed Charge, 3. Fixed Rate */

  /* If penalty option selected */
  if (nPenaltyOption <> 0) then

    /***********************************************************************/
    /* 1.FIRST CALCULATE OF AMOUNT TO CHARGE PENALTY ACCORDING TO METHOD   */
    /***********************************************************************/

    /* Penalies on Overdue Amounts */
    if (nOpenItems = 0) then

      if (nPenaltyOption <> 1) then

        update TMP_CUST_STATM
           set CUR_PAYMENTS = 0;

    delete from TMP_CUST_STATM_PENALTY;

    insert into TMP_CUST_STATM_PENALTY(CUSTKEY, STATM_NO, TRNS_TYPE, TRNS_STYPE, AMOUNT, DOCUMNT_NO)
     select ft.CUSTKEY, ft.STATM_NO, ft.TRNS_TYPE, ft.TRNS_STYPE, ft.AMOUNT, ft.DOCUMNT_NO
       from TMP_CUST_STATM t1, VWF_TRANS ft
          where ft.CUSTKEY  = t1.CUSTKEY
            and ft.STATM_NO = t1.NEW_STATM_NO;

    commit;

        /* we already have CL_BLNCE from previous statement */
        /* get current payments from current statment*/
        update TMP_CUST_STATM t
           set CUR_PAYMENTS = (select coalesce( cp.CUR_PAY , 0 )
                                 from (select t1.CUSTKEY,
                                              sum(ft.AMOUNT) as CUR_PAY
                                         from TMP_CUST_STATM t1, TMP_CUST_STATM_PENALTY ft
                                        where ft.CUSTKEY  = t1.CUSTKEY
                                          and ft.STATM_NO = t1.NEW_STATM_NO
                                          and (
                                               (ft.TRNS_TYPE =  kCreditNote)  or
                                               (ft.TRNS_TYPE =  kJournals   and ft.TRNS_STYPE = 2) or
                                               (ft.TRNS_TYPE =  kReceipting and ft.TRNS_STYPE = kReceiptingCons) or
                                               (ft.TRNS_TYPE =  kReceipting and ft.TRNS_STYPE >= 60)
                                               )
                                        group by t1.CUSTKEY ) cp
                                where t.CUSTKEY = cp.CUSTKEY);

        /* add related charges from current statement */
        update TMP_CUST_STATM t
           set CUR_PAYMENTS = CUR_PAYMENTS +
                           nvl((select coalesce ( rc.RCT_CHARGES , 0 )
                                  from (select t1.custkey,
                                               sum(f.AMOUNT) as RCT_CHARGES
                                          from TMP_CUST_STATM t1, TMP_CUST_STATM_PENALTY f
                                         where f.CUSTKEY  = t1.CUSTKEY
                                           and f.STATM_NO = t1.NEW_STATM_NO
                                           and f.TRNS_TYPE = kReceipting
                                           and f.TRNS_STYPE IN ( kRcptCharge1,
                                                                 kRcptCharge2,
                                                                 kRcptCharge3,
                                                                 kRcptCharge4,
                                                                 kRcptCharge5 )
                                           and exists ( select 1
                                                          from TMP_CUST_STATM_PENALTY f2
                                                         where f.CUSTKEY    = f2.CUSTKEY
                                                           and f.STATM_NO   = f2.STATM_NO
                                                           and f.DOCUMNT_NO = f2.DOCUMNT_NO
                                                           and (
                                                                (f2.TRNS_TYPE =  kCreditNote)  or
                                                                (f2.TRNS_TYPE =  kJournals   and f2.TRNS_STYPE = 2) or
                                                                (f2.TRNS_TYPE =  kReceipting and f2.TRNS_STYPE = kReceiptingCons) or
                                                                (f2.TRNS_TYPE =  kReceipting and f2.TRNS_STYPE >= 60)
                                                                )
                                                       )
                                        group by t1.CUSTKEY
                                       ) rc
                                where t.CUSTKEY = rc.CUSTKEY),0);

     update TMP_CUST_STATM t
            set CUR_PAYMENTS = 0
      where  CUR_PAYMENTS is null;

      elsif ( nPenaltyOption = 1 ) then

       /* Penalty is based on a previous statement according to setting in LU_CONSUMERTYPES */
        update TMP_CUST_STATM
           set CUR_PAYMENTS = 0,
               CL_BLNCE_PEN = 0,
               TRANS_CNT    = 0;

        /* Get the Penalty option cycle period */
        update TMP_CUST_STATM t
           set TRANS_CNT = ( select b.PNLTY_BCYC
                               from LU_CONSUMERTYPES b
                              where t.CONSUMER_TYPE = b.CODE )
         where exists (select 1 from LU_CONSUMERTYPES b
                        where t.CONSUMER_TYPE = b.CODE );

        delete from TMP_CUST_STATM_PENALTY;

     insert into TMP_CUST_STATM_PENALTY(CUSTKEY, STATM_NO, TRNS_TYPE, TRNS_STYPE, AMOUNT, DOCUMNT_NO)
     select ft.CUSTKEY, ft.STATM_NO, ft.TRNS_TYPE, ft.TRNS_STYPE, ft.AMOUNT, ft.DOCUMNT_NO
       from TMP_CUST_STATM t1, VWF_TRANS ft
          where ft.CUSTKEY  =  t1.CUSTKEY
            and ft.STATM_NO >= t1.NEW_STATM_NO
            and ft.STATM_NO <  t1.NEW_STATM_NO + t1.TRANS_CNT;

    commit;

        /* Get closing balance of appropriate statement */
        update TMP_CUST_STATM t
           set CL_BLNCE_PEN = (select coalesce(fs.CL_BLNCE , 0)
                                 from vwF_STATM fs
                                where fs.CUSTKEY   = t.CUSTKEY
                                  and fs.STATM_NO  = t.NEW_STATM_NO + TRANS_CNT
                                  and fs.SUBSTM_NO = 1);

        /* get payments since specified statement */
        update TMP_CUST_STATM t
           set CUR_PAYMENTS = (select cp.CUR_PAY
                                 from ( select t1.CUSTKEY,
                                               sum(ft.AMOUNT) as CUR_PAY
                                          from TMP_CUST_STATM t1, TMP_CUST_STATM_PENALTY ft
                                         where ft.CUSTKEY  =  t1.CUSTKEY
                                           and ft.STATM_NO >= t1.NEW_STATM_NO
                                           and ft.STATM_NO <  t1.NEW_STATM_NO + t1.TRANS_CNT
                                           and (
                                                (ft.TRNS_TYPE =  kCreditNote)  or
                                                (ft.TRNS_TYPE =  kJournals   and ft.TRNS_STYPE = 2) or
                                                (ft.TRNS_TYPE =  kReceipting and ft.TRNS_STYPE = kReceiptingCons) or
                                                (ft.TRNS_TYPE =  kReceipting and ft.TRNS_STYPE >= 60)
                                                )
                                         group by t1.CUSTKEY ) cp
                                where t.CUSTKEY = cp.CUSTKEY);

        /* add related charges */
        update TMP_CUST_STATM t
           set CUR_PAYMENTS = CUR_PAYMENTS + nvl((select coalesce (rc.RCT_CHRG , 0 )
                                                from (select t1.CUSTKEY,
                                                             sum(f.AMOUNT) as RCT_CHRG
                                                        from TMP_CUST_STATM t1, TMP_CUST_STATM_PENALTY f
                                                       where f.CUSTKEY  = t1.CUSTKEY
                                                         and f.STATM_NO >= t1.NEW_STATM_NO
                                                         and f.STATM_NO <  t1.NEW_STATM_NO + t1.TRANS_CNT
                                                         and f.TRNS_TYPE = kReceipting
                                                         and f.TRNS_STYPE IN ( kRcptCharge1,
                                                                               kRcptCharge2,
                                                                               kRcptCharge3,
                                                                               kRcptCharge4,
                                                                               kRcptCharge5 )
                                                         and exists ( select 1
                                                                        from TMP_CUST_STATM_PENALTY f2
                                                                       where f.CUSTKEY    = f2.CUSTKEY
                                                                         and f.STATM_NO   = f2.STATM_NO
                                                                         and f.DOCUMNT_NO = f2.DOCUMNT_NO
                                                                         and (
                                                                             (f2.TRNS_TYPE =  kCreditNote)  or
                                                                             (f2.TRNS_TYPE =  kJournals   and f2.TRNS_STYPE = 2) or
                                                                             (f2.TRNS_TYPE =  kReceipting and f2.TRNS_STYPE = kReceiptingCons) or
                                                                             (f2.TRNS_TYPE =  kReceipting and f2.TRNS_STYPE >= 60)
                                                                             )
                                                                      )
                                                       group by t1.CUSTKEY
                                                     ) rc
                                               where t.CUSTKEY = rc.CUSTKEY),0);

        update TMP_CUST_STATM
           set CUR_PAYMENTS = 0
     where CUR_PAYMENTS is null;

      update TMP_CUST_STATM
           set CL_BLNCE_PEN = 0
     where CL_BLNCE_PEN is null;

    update TMP_CUST_STATM
           set TRANS_CNT = 0
     where TRANS_CNT is null;

     end if; /* nPenaltyOption <> 1 */

   end if;   /* nOpenItems = 0 */


   /********************************************************************************/
   /* Else Penalties on Open Item Acctng                                           */
   /********************************************************************************/
   if (nOpenItems = 1) then

    begin
     /* Find the cycle to base calculations from */
     select nvl(CYCLE_ID,-99) into nPrevCycleId
       from SUM_BCYC
      where BILLGROUP = p_strBillGroup
        and BILNG_DATE = ( select max(BILNG_DATE)
                             from SUM_BCYC
                            where BILLGROUP = p_strBillGroup
                              and BILNG_DATE < p_dtBillCycleDate );

    exception
      when OTHERS then
        raise_application_error( -20969, 'Previous Billing Cycle not found for penalty calculation' );
        rollback;
        PROGRESS.END_OF_PROCESS( p_nProcessId );
        return;
    end;

   end if;

   /*********************************************************************/
   /* 2.CALCULATION OF PENALTY AMOUNT AND ISSUE OF PENALTY TRANSACTION  */
   /*********************************************************************/

   /***********************************************************************************/
   declare
   /* Identify Transactions to be issued with Penalties */
     cursor PENALTIES_CURSOR1 is
       select F.CUSTKEY,
              C.NEW_STATM_NO,
              F.PROP_REF,
              F.AMOUNT - coalesce(F.ALLOCATED_AMOUNT,0) as AMOUNT,
              coalesce(CT.PNLTY_CHRG,0) as PNLTY_CHRG
         from VWF_TRANS F, TMP_CUST_STATM C, LU_CONSUMERTYPES CT
        where F.CUSTKEY = C.CUSTKEY
          and C.CONSUMER_TYPE = CT.CODE
          and F.AMOUNT > 0
          and F.BILL_CYCLE_ID = nPrevCycleId
          and F.BILL_CYCLE_STEP = 3
          and F.AMOUNT - coalesce(F.ALLOCATED_AMOUNT, 0)  > coalesce( ct.PNLTY_LIMT, 0 )
          and F.ALLOCATED_AMOUNT >= 0
          and coalesce(CT.PNLTY_CHRG,0) <> 0
        order by F.CUSTKEY, F.PROP_REF;

     cursor PENALTIES_CURSOR2 is
     select c.CUSTKEY,
              c.NEW_STATM_NO,
              NULL as PROP_REF,
              (c.OP_BLNCE + c.CUR_PAYMENTS) as AMOUNT,
              coalesce(ct.PNLTY_CHRG,0) as PNLTY_CHRG
         from TMP_CUST_STATM c, LU_CONSUMERTYPES ct
        where c.CONSUMER_TYPE = ct.CODE
          and c.OP_BLNCE + c.CUR_PAYMENTS > coalesce( ct.PNLTY_LIMT, 0 )
          and coalesce(ct.PNLTY_CHRG,0) <> 0
        order by c.CUSTKEY;

   cursor PENALTIES_CURSOR3 is
     select c.CUSTKEY,
              c.NEW_STATM_NO,
              NULL as PROP_REF,
             (c.CL_BLNCE_PEN + c.CUR_PAYMENTS) as AMOUNT,
             coalesce(ct.PNLTY_CHRG,0) as PNLTY_CHRG
        from TMP_CUST_STATM c, LU_CONSUMERTYPES ct
        where c.CONSUMER_TYPE = ct.CODE
          and c.CL_BLNCE_PEN + c.CUR_PAYMENTS >  coalesce( ct.PNLTY_LIMT, 0 )
          and coalesce(ct.PNLTY_CHRG,0) <> 0
        order by c.CUSTKEY;


   begin

    /* Issue Penalties */
    select count(1)
    into nTotRecords
      from TMP_CUST_STATM;

    nTotRecords := nvl(nTotRecords,0);
    PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
    nCount := 0;
    nIncrement := floor(nTotRecords / 50);
    if nIncrement < 1 then
      nIncrement := 1;
    end if;

  if nOpenItems = 1 then
     open PENALTIES_CURSOR1;
  elsif ( nOpenItems = 0 and nPenaltyOption <> 1 ) then
     open PENALTIES_CURSOR2;
  elsif ( nOpenItems = 0 and nPenaltyOption = 1 ) then
     open PENALTIES_CURSOR3;
  end if;

    LOOP

     if nOpenItems = 1 then
     fetch PENALTIES_CURSOR1 into strCustKey, nNewStatmNo, strPropRef, nAmount, nPenaltyCharge;
     exit when PENALTIES_CURSOR1%NOTFOUND;
    elsif ( nOpenItems = 0 and nPenaltyOption <> 1 ) then
     fetch PENALTIES_CURSOR2 into strCustKey, nNewStatmNo, strPropRef, nAmount, nPenaltyCharge;
     exit when PENALTIES_CURSOR2%NOTFOUND;
   elsif ( nOpenItems = 0 and nPenaltyOption = 1 ) then
     fetch PENALTIES_CURSOR3 into strCustKey, nNewStatmNo, strPropRef, nAmount, nPenaltyCharge;
     exit when PENALTIES_CURSOR3%NOTFOUND;
   end if;

      /* Display Progress */
      if nCount = nIncrement then
        PROGRESS.START_NEW_RECORD (p_nProcessId, strCustKey, nIncrement);
        commit;
        nCount := 0;
      end if;
      nCount := nCount + 1;

      /* Calculate Penalty based on the Consumer Type */
      nPenalty := nAmount;

      if    (nPenaltyOption = 2) then /* Fixed Charge */

     nPenalty := nPenaltyCharge;

    elsif (nPenaltyOption = 3) then  /* Fixed Rate */

     nPenalty := nPenalty * nPenaltyCharge * 0.01;

    elsif (nPenaltyOption = 1) then /* Unpaid since specific biling */

       /*** Find Billing date of appropriate statement */
       select coalesce(fs.BILNG_DATE , p_dtBillCycleDate) into ndtPrevBilngDte
         from vwF_STATM fs, TMP_CUST_STATM t
        where fs.CUSTKEY   = strCustKey
          and fs.CUSTKEY   = t.CUSTKEY
          and fs.STATM_NO  = t.NEW_STATM_NO + t.TRANS_CNT
          and fs.SUBSTM_NO = 1;

        nPenalty := nPenalty * nPenaltyCharge * 0.01 * ( (p_dtBillCycleDate - ndtPrevBilngDte) / 365 );

      end if;

      /* Do Rounding For Penalty */
      nAmountDiff := 0;
      Billing.RoundTo (nRoundTo, nNoOfDigits, nRoundingMethod, nPenalty, nAmountDiff);

      /*If greater than the limit then issue a transaction*/
      if  ( nPenalty > 0.01 ) then

      Billing_Steps.PostRoundingDifference  (nPenalty,
                                             strCustKey,
                                             p_strBillGroup,
                                             nNewStatmNo,
                                             p_dtBillCycleDate,
                                             p_dtBillCycleDate,
                                             p_nBillCycleId,
                                             kDebitNote,
                                             kDebitPenalty,
                                             p_strCurrentUser,
                                             NULL,
                                             strPropRef,
                                             nAmount, /* Amount on which penalty is based */
                                             nNewTransNo);


      end if;

    end loop;

    if nOpenItems = 1 then
     close PENALTIES_CURSOR1;
   elsif ( nOpenItems = 0 and nPenaltyOption <> 1 ) then
     close PENALTIES_CURSOR2;
  elsif ( nOpenItems = 0 and nPenaltyOption = 1 ) then
     close PENALTIES_CURSOR3;
  end if;

   end;
   /***************************************************************************************/

   if (nOpenItems = 0) then  /* Reset these fields again */
    update TMP_CUST_STATM
       set CUR_PAYMENTS = 0,  /** Used to store the cur_payments for calc. penalty      ***/
           CL_BLNCE_PEN = 0,  /** Used to store cl_blnce of approp statment for pen.calc **/
           TRANS_CNT    = 0;  /** Used to store no. approp statment back ***/

   end if;


  end if; /** if (@nPenaltyOption <> 0) **/

  /*******************************************************************************************************/
  /***  3.CALCULATE AND PERFORM ROUNDING ON CLOSING BALANCE                                            ***/
  /*******************************************************************************************************/

  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  select count(1)
  into nTotRecords
    from TMP_CUST_STATM;

  nTotRecords := nvl(nTotRecords,0);
  PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
  nCount := 0;
  nIncrement := floor(nTotRecords / 50);
  if nIncrement < 1 then
    nIncrement := 1;
  end if;

  for STATMENT_CURSOR in (select CUSTKEY, NEW_STATM_NO, OP_BLNCE
                            from TMP_CUST_STATM)
  loop

    /* Display Progress */
    if nCount = nIncrement then
       PROGRESS.START_NEW_RECORD (p_nProcessId, STATMENT_CURSOR.CUSTKEY , nIncrement);
       commit;
        nCount := 0;
    end if;
    nCount := nCount + 1;

  /* get sum of Transactions for new statement */
  select SUM(ft.AMOUNT)
    into nBalance
      from VWF_TRANS ft
   where ft.CUSTKEY = STATMENT_CURSOR.CUSTKEY
     and ft.STATM_NO = STATMENT_CURSOR.NEW_STATM_NO;

  /* add the opening balance */
  nBalance := STATMENT_CURSOR.OP_BLNCE + nvl(nBalance,0);

  /* Do Rounding For Closing Balance and Finding Difference */
    nBalanceDiff := 0;
    Billing.RoundTo (nRoundTo, nNoOfDigits, nRoundingMethod, nBalance, nBalanceDiff);

    /* If there is a rounding difference issue a transaction */
    if ( nBalanceDiff <> 0 ) then


        /* Issue Rounding Difference Transaction */
        nTrnsType := case when nBalanceDiff > 0
                          then kDebitNote
                          else kCreditNote
                     end;

        Billing_Steps.PostRoundingDifference (nBalanceDiff,
                                              STATMENT_CURSOR.CUSTKEY,
                                              p_strBillGroup,
                                              STATMENT_CURSOR.NEW_STATM_NO,
                                              p_dtBillCycleDate,
                                              p_dtBillCycleDate ,
                                              p_nBillCycleId,
                                              nTrnsType,
                                              kRoundingDiff,
                                              p_strCurrentUser,
                                              NULL,
                                              NULL,
                                              NULL,
                                              nNewTransNo);

    end if;

    /* Update Temp Table With The Rounded Balance */
    update TMP_CUST_STATM
       set CL_BLNCE_ROUNDED = nBalance
     where CUSTKEY = STATMENT_CURSOR.CUSTKEY;

  end loop;


  /*******************************************************************************************************/
  /***  4.GROUP ACCOUNTS PROCESSING                                                                    ***/
  /*******************************************************************************************************/

  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  /*
  SELECT   cs.custkey, cs.cur_charges,
                                        cd.group_ref
                                   FROM tmp_cust_statm cs, cust_dtl cd
                                  WHERE cs.custkey = cd.custkey
                                    AND cd.acc_type = 1
                               ORDER BY cs.custkey
  */

  select count(1)
  into nTotRecords
    from TMP_CUST_STATM t, CUST_DTL cd
   where t.CUSTKEY = cd.CUSTKEY
   and cd.acc_type = 1 /* cd.GROUP_REF is not null */
     and exists (select 1 from CUST_DTL z where z.CUSTKEY = cd.GROUP_REF);

  nTotRecords := nvl(nTotRecords,0);
  PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
  nCount := 0;
  nIncrement := floor(nTotRecords / 50);
  if nIncrement < 1 then
    nIncrement := 1;
  end if;

  for GRP_ACCTS_CURSOR in
   (select t.CUSTKEY, cd.GROUP_REF, t.NEW_STATM_NO, t.OP_BLNCE
      from TMP_CUST_STATM t, CUST_DTL cd
   where t.CUSTKEY = cd.CUSTKEY
     and cd.acc_type = 1 /* cd.GROUP_REF is not null */
       and exists (select 1 from CUST_DTL z where z.CUSTKEY = cd.GROUP_REF))
  loop

    /* Display Progress */
    if nCount = nIncrement then
       PROGRESS.START_NEW_RECORD (p_nProcessId, GRP_ACCTS_CURSOR.CUSTKEY , nIncrement);
       commit;
     nCount := 0;
    end if;
    nCount := nCount + 1;

  select sum(ft.AMOUNT)
    into nBalance
      from vwF_TRANS ft
     where ft.CUSTKEY  = GRP_ACCTS_CURSOR.CUSTKEY
     and ft.STATM_NO = GRP_ACCTS_CURSOR.NEW_STATM_NO;

  nBalance := GRP_ACCTS_CURSOR.OP_BLNCE + nvl(nBalance,0);

    if ( nBalance > 0 ) then

    begin
       select BILLGROUP into strGrpBilGrp
         from CUST_DTL
        where CUSTKEY = GRP_ACCTS_CURSOR.GROUP_REF;
    exception
       when OTHERS then
         raise_application_error( -20971, 'Billing group for Group account not found' );
         rollback;
         PROGRESS.END_OF_PROCESS( p_nProcessId );
         return;
      end;

      /****************************************************************/
      /* Insert Credit for Account                                    */
      /****************************************************************/

      nBalance := ABS(nBalance) * -1;

      Billing_Steps.PostRoundingDifference (nBalance,
                                            GRP_ACCTS_CURSOR.CUSTKEY,
                                            p_strBillGroup,
                                            GRP_ACCTS_CURSOR.NEW_STATM_NO,
                                            p_dtBillCycleDate,
                                            p_dtBillCycleDate,
                                            p_nBillCycleId,
                                            kJournals,
                                            kJournalsCredit,
                                            p_strCurrentUser,
                                            GRP_ACCTS_CURSOR.GROUP_REF,
                                            NULL,
                                            NULL,
                                            nNewTransNo);

      nBalance := ABS(nBalance);

      if ( nOpenItems = 1 ) then
        Billing_Steps.AllocateCharges (GRP_ACCTS_CURSOR.CUSTKEY, p_nBillCycleId, nNewTransNo, nBalance, p_strCurrentUser);
      end if;

      /** Make the customers statement cl blnce = zero **/
      update TMP_CUST_STATM
         set CL_BLNCE_ROUNDED = CL_BLNCE_ROUNDED - nBalance
        where CUSTKEY = GRP_ACCTS_CURSOR.CUSTKEY;

      /******************************************************************************/
      /* Insert Debit for Group Account                                             */
      /******************************************************************************/

      begin
       select nvl(NEW_STATM_NO,0) into nStatmNo
         from TMP_CUST_STATM
        where custkey = GRP_ACCTS_CURSOR.GROUP_REF;
      exception
       when OTHERS then
         nStatmNo:= 0;
      end;

      if ( strGrpBilGrp <> p_strBillGroup ) then
         dtGrpEffect := NULL;
       else
         dtGrpEffect := p_dtBillCycleDate;
    end if;

      Billing_Steps.PostRoundingDifference (nBalance,
                                            GRP_ACCTS_CURSOR.GROUP_REF,
                                            strGrpBilGrp,
                                            nStatmNo,
                                            dtGrpEffect,
                                            p_dtBillCycleDate,
                                            p_nBillCycleId,
                                            kJournals,
                                            kJournalsDebit,
                                            p_strCurrentUser,
                                            GRP_ACCTS_CURSOR.CUSTKEY,
                                            NULL,
                                            NULL,
                                            nNewTransNo);


      if ( strGrpBilGrp = p_strBillGroup ) then

       /** If group account in billgroup for which we are generating stmts **/
       /** then add to cl-blnce the new trans debited to the account       **/
       update TMP_CUST_STATM
          set CL_BLNCE_ROUNDED = CL_BLNCE_ROUNDED + nBalance
        where CUSTKEY = GRP_ACCTS_CURSOR.GROUP_REF;

      end if;

    end if; /* nBalance > 0 */

  end loop;


  /*******************************************************************************************************/
  /***  5.GROUPING OF TRANSACTIONS                                                                     ***/
  /*******************************************************************************************************/

  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  select count(1) into nTotRecords from TMP_CUST_STATM;

  nTotRecords := nvl(nTotRecords,0);
  PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
  nCount := 0;
  nIncrement := floor(nTotRecords / 50);
  if nIncrement < 1 then
    nIncrement := 1;
  end if;

  for TMP_CUST_CURSOR in (select CUSTKEY, NEW_STATM_NO from TMP_CUST_STATM)
  loop

    /* Display Progress */
    if nCount = nIncrement then
       PROGRESS.START_NEW_RECORD (p_nProcessId, TMP_CUST_CURSOR.CUSTKEY , nIncrement);
       commit;
     nCount := 0;
    end if;
    nCount := nCount + 1;

    update TMP_CUST_STATM t
      set (TRANS_CNT,
         CL_BLNCE,
       CUR_CHARGES,
       CUR_PAYMENTS
      ) =
          (select count(1),
               coalesce(t.OP_BLNCE,0) + coalesce(sum(ft.AMOUNT),0),
          sum( case when (ft.BILL_CYCLE_ID = p_nBillCycleId)
                            then ft.AMOUNT
                            else 0
                        end ),
          sum( case when ( ( ft.TRNS_TYPE  = kReceipting ) and
                                   ( ft.TRNS_STYPE = kReceiptingCons ) )
                            then ft.AMOUNT
                            else 0
                        end )
             from vwF_TRANS ft
            where ft.CUSTKEY = t.CUSTKEY
              and ft.STATM_NO = t.NEW_STATM_NO)
     where t.CUSTKEY = TMP_CUST_CURSOR.CUSTKEY;

    /* Set receipting charges as current payments if they correspond to current payment transactions */
   update TMP_CUST_STATM t
        set CUR_PAYMENTS = coalesce(CUR_PAYMENTS,0)
                      +
            coalesce(
            (select sum(ft.AMOUNT)
                             from VWF_TRANS ft
                            where ft.CUSTKEY = t.CUSTKEY
                              and ft.STATM_NO = t.NEW_STATM_NO
                              and ft.TRNS_TYPE = kReceipting
                              and ft.TRNS_STYPE IN ( kRcptCharge1,
                                                     kRcptCharge2,
                                                     kRcptCharge3,
                                                     kRcptCharge4,
                                                     kRcptCharge5 )
                              and exists ( select 1
                                             from VWF_TRANS ft2
                                            where ft.CUSTKEY     = ft2.CUSTKEY
                                              and ft.STATM_NO    = ft2.STATM_NO
                                              and ft.DOCUMNT_NO  = ft2.DOCUMNT_NO
                                              and ft2.TRNS_TYPE  = kReceipting
                             and ft2.TRNS_STYPE = kReceiptingCons)
              )
               ,0)
      where t.CUSTKEY = TMP_CUST_CURSOR.CUSTKEY;

  end loop;

    update TMP_CUST_STATM t
       set TRANS_CNT = 0
   where TRANS_CNT is null;

  update TMP_CUST_STATM t
       set CL_BLNCE = 0
   where CL_BLNCE is null;

  update TMP_CUST_STATM t
       set CUR_CHARGES = 0
   where CUR_CHARGES is null;

  update TMP_CUST_STATM t
       set CUR_PAYMENTS = 0
   where CUR_PAYMENTS is null;

    update TMP_CUST_STATM
       set ADJUSTMENTS   = CL_BLNCE - (OP_BLNCE + CUR_CHARGES + CUR_PAYMENTS);

  /*******************************************************************************************************/
  /*** 6. INSTALLMENTS CALCULATION                                                                     ***/
  /*******************************************************************************************************/

  select nvl(KEYWORD_VALUE,0) into nInstSetting
    from SETTINGS
   where KEYWORD = 'BIL_INSTALLMENT_Method';

  select nvl(KEYWORD_VALUE,0) into nNoNegBalance
    from SETTINGS
   where KEYWORD = 'BIL_STATEMENTS_NoNegativeBalance';


  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  select count(1)
  into nTotRecords
    from TMP_CUST_STATM;

  nTotRecords := nvl(nTotRecords,0);
  PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
  nCount := 0;
  nIncrement := floor(nTotRecords / 50);
  if nIncrement < 1 then
    nIncrement := 1;
  end if;


  /** Installment is based on the ARREARS i.e. CL_BLNCE before CUR_CHARGES **/
  update TMP_CUST_STATM
     set INSTALLMENT = CL_BLNCE - CUR_CHARGES;

  for INSTALLMENT_CURSOR in
   (select CUSTKEY, CONSUMER_TYPE, INSTALLMENT, CUR_CHARGES
      from TMP_CUST_STATM
     order by CUSTKEY)
  loop

   /* Display Progress */
   if nCount = nIncrement then
       PROGRESS.START_NEW_RECORD (p_nProcessId, INSTALLMENT_CURSOR.CUSTKEY , nIncrement);
       commit;
     nCount := 0;
   end if;
   nCount := nCount + 1;

   if INSTALLMENT_CURSOR.CONSUMER_TYPE is null then
       raise_application_error( -20971, 'Installment Calculation did not find consumer type for customer' );
       rollback;
       PROGRESS.END_OF_PROCESS( p_nProcessId );
       return;
   end if;

   nStatmArrears := INSTALLMENT_CURSOR.INSTALLMENT;

   /* Adjust according to installment policy */
   if ( ( nInstSetting = kInstFixedAmt ) or
        ( nInstSetting = kInstPercent ) )
       and ( nStatmArrears > 0 ) then


     select (case when ( nStatmArrears >= INSTLMN_BAND1 ) and
                       ( nStatmArrears <  (case when INSTLMN_BAND2 > 0
                                      then INSTLMN_BAND2
                        else nStatmArrears + 1
                      end) )
                  then INSTALMENT1
                  when ( nStatmArrears >= INSTLMN_BAND2 ) and
                       ( nStatmArrears <  (case when INSTLMN_BAND3 > 0
                                      then INSTLMN_BAND3
                        else nStatmArrears + 1
                      end) )
                  then INSTALMENT2
                  when ( nStatmArrears >= INSTLMN_BAND3 ) and
                       ( nStatmArrears <  (case when INSTLMN_BAND4 > 0
                                      then INSTLMN_BAND4
                        else nStatmArrears + 1
                      end) )
                  then INSTALMENT3
                  when ( nStatmArrears >= INSTLMN_BAND4 ) and
                       ( nStatmArrears <  (case when INSTLMN_BAND5 > 0
                                      then INSTLMN_BAND5
                        else nStatmArrears + 1
                      end) )
                   then INSTALMENT4
                  when ( nStatmArrears >= INSTLMN_BAND5 )
                  then INSTALMENT5
                  else 0
              end ) * ( case when (nInstSetting = kInstPercent)
                             then (0.01 * nStatmArrears)
                             when (nInstSetting = kInstFixedAmt)
                             then 1
                        end )
     into nStatmArrears
       from LU_CONSUMERTYPES
      where CODE = INSTALLMENT_CURSOR.CONSUMER_TYPE;

   end if;

   /* Apply setting option */
   if ( nNoNegBalance = 1 and nStatmArrears < 0 ) then
      nStatmArrears := 0;
   end if;

   /* Before rounding we add the CUR_CHARGES so that the INSTALLMENT + CUR_CHARGE is rounded */
   nStatmArrears :=  nStatmArrears + INSTALLMENT_CURSOR.CUR_CHARGES;

   /* Do Rounding For Installment */
   Billing.RoundTo (nRoundTo, nNoOfDigits, nRoundingMethod, nStatmArrears, nStatmArrearsDiff);

   update TMP_CUST_STATM
      set INSTALLMENT = nStatmArrears - INSTALLMENT_CURSOR.CUR_CHARGES
    where CUSTKEY = INSTALLMENT_CURSOR.CUSTKEY;

  end loop;

  /*******************************************************************************************************/
  /*** 7. RECEIPTING CHARGES CALCULATION                                                               ***/
  /*******************************************************************************************************/

  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  select nvl(USE_RCPT_CHARGES,0) into nUseRcptChg
    from TRANSACTION_TYPES
   where TRNS_CODE  = kReceipting
     and TRNS_SCODE = kReceiptingCons;

  nRcptChrgCnt := 1;
  kRcptCharge := kRcptCharge1; /* Init to 51 */

  while ( nRcptChrgCnt <= kRcptChargesCnt )
  loop

    bitUseRcptChg := mod(nUseRcptChg,2);

    if ( bitUseRcptChg = 1 ) then

        strSQL:= 'update TMP_CUST_STATM t
                    set RCPT_CHARGE' || nRcptChrgCnt || '
                    = (select case when TT.CHRGTYPE = 0
                               then /* Fixed Charge */
                            TT.FIXED_CHARGE
                                             when TT.CHRGTYPE = 1
                       then /* Lookup charges */
                                                  coalesce( (select lup.FIXED
                                                               from CHARGE_LU_TABLE lup
                                                              where lup.CODE = t.CONSUMER_TYPE
                                                                and lup.TRNS_CODE  = ' || kReceipting || '
                                                                and lup.TRNS_SCODE = ' || kRcptCharge || ') ,0)
                                             when TT.CHRGTYPE = 3
                       then /* Percentage */
                                                  (case when TT.FIXED_CHARGE * 0.01 *
                                                             (t.INSTALLMENT + t.CUR_CHARGES) > TT.MIN_AMOUNT
                                                        then TT.FIXED_CHARGE * 0.01 *
                                                            (t.INSTALLMENT + t.CUR_CHARGES)
                                                        else TT.MIN_AMOUNT
                                                   end)
                                             else 0
                                         end
                                    from TRANSACTION_TYPES TT
                                   where TT.TRNS_CODE  = ' || kReceipting || '
                                     and TT.TRNS_SCODE = ' || kRcptCharge || ')' ;

      begin
        execute immediate strSQL;
      exception
        when OTHERS then
          raise_application_error( -20972, 'Error updating receipting charges' );
          rollback;
          PROGRESS.END_OF_PROCESS( p_nProcessId );
          return;
      end;

    end  if;

    nRcptChrgCnt := nRcptChrgCnt + 1;
    kRcptCharge  := kRcptCharge + 1;
    nUseRcptChg  := floor(nUseRcptChg / 2);

  end loop;

  /***************************************/
  /**** Rounding of Receipting Charges ***/

  select count(1)
  into nTotRecords
    from TMP_CUST_STATM;

  nTotRecords := nvl(nTotRecords,0);
  PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
  nCount := 0;
  nIncrement := floor(nTotRecords / 50);
  if nIncrement < 1 then
    nIncrement := 1;
  end if;

  for RCT_CURSOR in
   ( select CUSTKEY, RCPT_CHARGE1, RCPT_CHARGE2, RCPT_CHARGE3, RCPT_CHARGE4, RCPT_CHARGE5
       from TMP_CUST_STATM
      order by CUSTKEY )
  loop

    nRctChrg1 := RCT_CURSOR.RCPT_CHARGE1;
    nRctChrg2 := RCT_CURSOR.RCPT_CHARGE2;
    nRctChrg3 := RCT_CURSOR.RCPT_CHARGE3;
    nRctChrg4 := RCT_CURSOR.RCPT_CHARGE4;
    nRctChrg5 := RCT_CURSOR.RCPT_CHARGE5;

    /* Display Progress */
    if nCount = nIncrement then
       PROGRESS.START_NEW_RECORD (p_nProcessId, RCT_CURSOR.CUSTKEY , nIncrement);
       commit;
     nCount := 0;
    end if;
    nCount := nCount + 1;

    Billing.RoundTo (nRoundTo, nNoOfDigits, nRoundingMethod, nRctChrg1, Rounddiff);
    Billing.RoundTo (nRoundTo, nNoOfDigits, nRoundingMethod, nRctChrg2, Rounddiff);
    Billing.RoundTo (nRoundTo, nNoOfDigits, nRoundingMethod, nRctChrg3, Rounddiff);
    Billing.RoundTo (nRoundTo, nNoOfDigits, nRoundingMethod, nRctChrg4, Rounddiff);
    Billing.RoundTo (nRoundTo, nNoOfDigits, nRoundingMethod, nRctChrg5, Rounddiff);

    update TMP_CUST_STATM
       set RCPT_CHARGE1 = nRctChrg1,
           RCPT_CHARGE2 = nRctChrg2,
           RCPT_CHARGE3 = nRctChrg3,
           RCPT_CHARGE4 = nRctChrg4,
           RCPT_CHARGE5 = nRctChrg5
     where CUSTKEY = RCT_CURSOR.CUSTKEY;

  end loop;


  /*******************************************************************************************************/
  /*** 8. AGEIING ANALYSIS CALCULATION                                                                 ***/
  /*******************************************************************************************************/

  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  update TMP_CUST_STATM
     set AGE_CUR = 0,
         AGE_30  = 0,
         AGE_60  = 0,
         AGE_90  = 0,
         AGE_120 = 0,
         AGE_150 = 0,
         AGE_180 = 0,
         AGE_1YR = 0,
         AGE_2YR = 0;

  select count(1) into nTotRecords  from TMP_CUST_STATM where CL_BLNCE > 0;

  nTotRecords := nvl(nTotRecords,0);
  PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
  nCount := 0;
  nIncrement := floor(nTotRecords / 50);
  if nIncrement < 1 then
    nIncrement := 1;
  end if;

  for AGE_CURSOR in
   (select CUSTKEY
     from TMP_CUST_STATM
    where CL_BLNCE > 0
  order by CUSTKEY)
  loop

   /* Display Progress */
   if nCount = nIncrement then
       PROGRESS.START_NEW_RECORD (p_nProcessId, AGE_CURSOR.CUSTKEY , nIncrement);
       commit;
     nCount := 0;
   end if;
   nCount := nCount + 1;

    update TMP_CUST_STATM t
     set (AGE_CUR, AGE_30, AGE_60, AGE_90, AGE_120, AGE_150, AGE_180, AGE_1YR, AGE_2YR) =
            (select /* LT 30 */
                     sum(case when MONTHS_BETWEEN(p_dtBillCycleDate, ft.BILNG_DATE) = 0
                              then ft.AMOUNT else 0
                          end) as AGE_CUR,
          /* 30 - 60 */
                     sum(case when MONTHS_BETWEEN(p_dtBillCycleDate, ft.BILNG_DATE) = 1
                              then ft.AMOUNT else 0
                          end) as AGE_30,
          /* 60 - 90*/
                     sum(case when MONTHS_BETWEEN(p_dtBillCycleDate, ft.BILNG_DATE) = 2
                              then ft.AMOUNT else 0
                          end) as AGE_60,
          /* 90 - 120 */
                     sum(case when MONTHS_BETWEEN(p_dtBillCycleDate, ft.BILNG_DATE) = 3
                              then ft.AMOUNT else 0
                          end) as AGE_90,
          /* 120 - 150 */
                     sum(case when MONTHS_BETWEEN(p_dtBillCycleDate, ft.BILNG_DATE) = 4
                               then ft.AMOUNT else 0
                          end) as AGE_120,
          /* 150 - 180 */
                     sum(case when MONTHS_BETWEEN(p_dtBillCycleDate, ft.BILNG_DATE) = 5
                              then ft.AMOUNT else 0
                          end) as AGE_150,
           /* 180 - 365 */
                     sum(case when MONTHS_BETWEEN(p_dtBillCycleDate, ft.BILNG_DATE) between 6 and 11
                              then ft.AMOUNT else 0
                          end) as AGE_180,
          /* 365 - 731 */
                     sum(case when MONTHS_BETWEEN(p_dtBillCycleDate, ft.BILNG_DATE) between 12 and 23
                              then ft.AMOUNT else 0
                         end) as AGE_1YR,
          /* GT 731 */
                     sum(case when MONTHS_BETWEEN(p_dtBillCycleDate, ft.BILNG_DATE) >= 24
                              then ft.AMOUNT else 0
                         end) as AGE_2YR
               from vwF_TRANS ft
              where ft.CUSTKEY = t.CUSTKEY
                and ft.BILNG_DATE <= p_dtBillCycleDate
                and coalesce(ft.STATM_NO,0) <> 0
                and ft.amount > 0)
  where t.CUSTKEY = AGE_CURSOR.CUSTKEY;

   update TMP_CUST_STATM
      set AGE_CUR = case when (CL_BLNCE - AGE_CUR  > 0)  and
                              (AGE_30 +AGE_60 +AGE_90 +AGE_120 +AGE_150 +AGE_180 +AGE_1YR +AGE_2YR) = 0
                          then CL_BLNCE
                         when (CL_BLNCE - AGE_CUR  > 0)
                          then AGE_CUR
                         when (CL_BLNCE) > 0
                          then (CL_BLNCE)
                         else 0
                       end,
          AGE_30  = case when (CL_BLNCE - AGE_CUR - AGE_30 > 0)  and
                              (AGE_60 + AGE_90 + AGE_120 + AGE_150 + AGE_180 + AGE_1YR +AGE_2YR) = 0
                          then CL_BLNCE - AGE_CUR
                         when (CL_BLNCE - AGE_CUR - AGE_30 > 0)
                          then AGE_30
                         when (CL_BLNCE - AGE_CUR) > 0
                          then (CL_BLNCE - AGE_CUR)
                         else 0
                       end,
          AGE_60  = case when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 > 0) and
                              (AGE_90 + AGE_120 + AGE_150 + AGE_180 + AGE_1YR + AGE_2YR) = 0
                          then CL_BLNCE - AGE_CUR - AGE_30
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 > 0)
                          then AGE_60
                         when (CL_BLNCE - AGE_CUR - AGE_30) > 0
                          then (CL_BLNCE - AGE_CUR - AGE_30)
                         else 0
                    end,
          AGE_90  = case when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 > 0) and
                               (AGE_120 + AGE_150 + AGE_180 + AGE_1YR + AGE_2YR) = 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60)
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 > 0)
                          then AGE_90
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60) > 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60)
                         else 0
                    end,
          AGE_120 = case when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 > 0) and
                              ( AGE_150 + AGE_180 + AGE_1YR + AGE_2YR) = 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90)
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 > 0)
                          then AGE_120
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90) > 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90)
                         else 0
                    end,
          AGE_150 = case when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 > 0) and
                              ( AGE_180 + AGE_1YR + AGE_2YR ) = 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120)
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 > 0)
                          then AGE_150
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120) > 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120)
                         else 0
                    end,
          AGE_180 = case when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 - AGE_180 > 0) and
                              ( AGE_1YR + AGE_2YR ) = 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150)
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 - AGE_180 > 0)
                          then AGE_180
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150) > 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150)
                         else 0
                    end,
          AGE_1YR = case when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 - AGE_180 - AGE_1YR > 0) and
                              (AGE_2YR) = 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 - AGE_180)
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 - AGE_180 - AGE_1YR > 0)
                          then AGE_1YR
                         when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 - AGE_180) > 0
                          then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 - AGE_180)
                         else 0
                    end,
          AGE_2YR = case when (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 - AGE_180 - AGE_1YR > 0)
                         then (CL_BLNCE - AGE_CUR - AGE_30 - AGE_60 - AGE_90 - AGE_120 - AGE_150 - AGE_180 - AGE_1YR)
                         else 0
                    end
    where CUSTKEY = AGE_CURSOR.CUSTKEY;

  end loop;


  /*******************************************************************************************************/
  /*** 9. PAY-BY-DATE CALCULATION                                                                      ***/
  /*******************************************************************************************************/

  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  update TMP_CUST_STATM t
     set PAY_BY = coalesce ( ( select p_dtBillCycleDate +
                                       (case when TRANS_CNT = 0
                                              then 0
                                              else coalesce(ct.PAYBY_PERIOD,30)
                                          end)
                                 from LU_CONSUMERTYPES ct
                                where t.CONSUMER_TYPE = ct.CODE )
                , p_dtBillCycleDate );


  /*******************************************************************************************************/
  /*** 10. CALCULATION OF NEXT PAYMENT NO                                                              ***/
  /*******************************************************************************************************/

  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  /* strPaymentNoHdr :=  to_char( p_dtBillCycleDate, 'YY' ) || substr( '00' || p_nStation, -2, 2 );

  select nvl( to_number( substr( max(PAYMENT_NO), 5, kStatmNoLen ) ), 0 ) + 1
    into nNextPaymentNo
    from vwF_STATM
   where PAYMENT_NO like strPaymentNoHdr || '%'; 
   */
   
   ------------------------ EGYPT 2020 --------------------------
   
   strPaymentNoHdr :=  to_char( 12 *( extract( year from p_dtBillCycleDate  )-2020 )+( extract( month from p_dtBillCycleDate ) + 12 )  ,'000');

   update TMP_CUST_STATM SET PAYMENT_NO = CUSTKEY || TRIM(strPaymentNoHdr) ;
  

  /*******************************************************************************************************/
  /*** 11. CREATION OF STATEMENTS                                                                      ***/
  /*******************************************************************************************************/

  PROGRESS.START_NEW_PHASE( p_nProcessId, 0, nContinue );
  COMMIT;
  if nContinue = 0 then
    rollback;
    PROGRESS.END_OF_PROCESS( p_nProcessId );
    return;
  end if;

  update TMP_CUST_STATM
     set TRANS_CNT = 0;

  /* Get the number of transaction for new statement per customer */
  update TMP_CUST_STATM t
     set TRANS_CNT = coalesce( ( select count(1)
                                   from VWF_TRANS tr
                                  where tr.CUSTKEY  = t.CUSTKEY
                                    and tr.STATM_NO = t.NEW_STATM_NO ), 0 );

  nSubStatmNo  := 1;
  nRowsUpdated := 1;

  /* Insert new statement records */
  while nRowsUpdated > 0
  loop

    select count(1) into nRowsUpdated
      from TMP_CUST_STATM
     where ( TRANS_CNT > ( nSubStatmNo - 1 ) * kMaxTrnsCnt )
        or ( TRANS_CNT = 0 AND nSubStatmNo = 1 AND OP_BLNCE <> 0 );

    if nRowsUpdated > 0 then

    insert into F_STATM
    (
      BILLGROUP,
      CUSTKEY,
      STATM_NO,
      SUBSTM_NO,
      BILNG_DATE,
      PAY_BY,
      BAL_DATE,
      OP_BLNCE,
      CL_BLNCE,
      INSTALMENT,
      OP_BLNCE_V,
      CL_BLNCE_V,
      CUR_CHARGES,
      CUR_PAYMNTS,
      TRANS_NO,
      PAYMENT_NO,
      AGE_CUR,
      AGE_30,
      AGE_60,
      AGE_90,
      AGE_120,
      AGE_150,
      AGE_180,
      AGE_1YR,
      AGE_2YR,
      RCPT_CHARGE1,
      RCPT_CHARGE2,
      RCPT_CHARGE3,
      RCPT_CHARGE4,
      RCPT_CHARGE5,
      TRANS_RF1,
      TRANS_RF2,
      TRANS_RF3,
      TRANS_RF4,
      TRANS_RF5,
      TRANS_RF6,
      TRANS_RF7,
      TRANS_RF8,
      TRANS_RF9,
      TRANS_RF10,
      STAMP_DATE,
      STAMP_USER
    )
    select p_strBillGroup,
           CUSTKEY,
           NEW_STATM_NO,
           nSubStatmNo,
           p_dtBillCycleDate,
           PAY_BY,
           p_dtBillCycleDate,
           coalesce(OP_BLNCE,0),
           coalesce(CL_BLNCE,0),
           coalesce(INSTALLMENT, 0),
           coalesce(OP_BLNCE,0),
           coalesce(CL_BLNCE,0),
           coalesce(CUR_CHARGES,0),
           coalesce(CUR_PAYMENTS,0),
           coalesce(TRANS_CNT,0),
           PAYMENT_NO,
           coalesce(AGE_CUR,0),
           coalesce(AGE_30,0),
           coalesce(AGE_60,0),
           coalesce(AGE_90,0),
           coalesce(AGE_120,0),
           coalesce(AGE_150,0),
           coalesce(AGE_180,0),
           coalesce(AGE_1YR,0),
           coalesce(AGE_2YR,0),
           coalesce(RCPT_CHARGE1,0),
           coalesce(RCPT_CHARGE2,0),
           coalesce(RCPT_CHARGE3,0),
           coalesce(RCPT_CHARGE4,0),
           coalesce(RCPT_CHARGE5,0),
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           0,
           sysdate,
           p_strCurrentUser
      from TMP_CUST_STATM
     where ( TRANS_CNT > ( nSubStatmNo - 1 ) * kMaxTrnsCnt )
        or ( TRANS_CNT = 0 AND nSubStatmNo = 1 AND OP_BLNCE <> 0 );

    end if;

    nSubStatmNo  := nSubStatmNo + 1;

  end loop;

  /** Update the TRANS_TF Nos. **/
  select count(1)
  into nTotRecords
    from TMP_CUST_STATM;

  nTotRecords := nvl(nTotRecords,0);
  PROGRESS.INITIALISE_RECORD (p_nProcessId, nTotRecords);
  nCount := 0;
  nIncrement := floor(nTotRecords / 50);
  if nIncrement < 1 then
    nIncrement := 1;
  end if;

  nCount := 0;

  for STATMENT_REF_CURSOR in
   (select CUSTKEY, NEW_STATM_NO from TMP_CUST_STATM)
  loop

    /* Display Progress */
    if nCount = nIncrement then
       PROGRESS.START_NEW_RECORD (p_nProcessId, STATMENT_REF_CURSOR.CUSTKEY , nIncrement);
       commit;
       nCount := 0;
    end if;
    nCount := nCount + 1;

    strTmpCustKey := STATMENT_REF_CURSOR.CUSTKEY;
    nSubStatmNo := 1;
    nTrans := 1;

    for TRANS_CURSOR in
    (select TRANS_NO
        from VWF_TRANS
       where CUSTKEY = STATMENT_REF_CURSOR.CUSTKEY
         and STATM_NO = STATMENT_REF_CURSOR.NEW_STATM_NO
       order by TRANS_NO desc)
    loop

      if nTrans > 10 then
        nSubStatmNo := nSubStatmNo + 1;
        nTrans := 1;
      end if;

      strSQL := 'update F_STATM ' ||
              '   set TRANS_RF' || nTrans || ' = ' || TRANS_CURSOR.TRANS_NO ||
                ' where F_STATM.CUSTKEY = ' || '''' ||  STATMENT_REF_CURSOR.CUSTKEY || '''' ||
                '   and F_STATM.STATM_NO = ' || STATMENT_REF_CURSOR.NEW_STATM_NO ||
                '   and F_STATM.SUBSTM_NO = ' || nSubStatmNo;

      begin
         execute immediate strSQL;
      exception
         when OTHERS then
         raise_application_error( -20973, SQLERRM );
         rollback;
         PROGRESS.END_OF_PROCESS( P_NPROCESSID );
         return;
      end;

      nTrans := nTrans + 1;

  end loop;

  end loop;

  /* Customer User queries sp */
  billing_steps.generatestmtuser (p_strbillgroup, p_dtbillcycledate, p_nbillcycleid, p_nstation,  p_strcurrentuser, p_nprocessid);

  PROGRESS.END_OF_PROCESS( p_nProcessId );
  return;

  commit;
  end GENERATESTATEMENTS;

/* ===========================================================================
   ===========================================================================
   ====  Billing Step 3: Generate Charges  ===================================
   ===========================================================================
   =========================================================================== */

   PROCEDURE invoicesforconsumption (
      p_nbillcycleid   NUMBER,
      p_nprocessid     NUMBER,
      p_strcurruser    NVARCHAR2,
      p_strCustkey     nvarchar2
   )
   AS /* Executable SQL string */
      strsql                     VARCHAR2 (4000); /* Progress Monitor Variables */
      nretval                    NUMBER (10);
      ntotrecords                NUMBER (10);
      ncount                     NUMBER (10);
      nincrement                 NUMBER (10); /* Cycle Variables */
      kthisstep                  NUMBER (10);
      kundefinedcycle            NUMBER (10);
      ncyclelengthmonths         NUMBER (10);
      ncyclelengthdays           NUMBER (20, 8);
      dtcyclecutoffday           DATE;
      dtcyclestartdate           DATE;
      strbillgroup               NVARCHAR2 (10); /* Service Number constants */
      kwatersrv                  NUMBER (10);
      ksewersrv                  NUMBER (10);
      kelectsrv                  NUMBER (10);
      krefsrv                    NUMBER (10);
      kboreholesrv               NUMBER (10);
      kratestaxsrv               NUMBER (10);
      kuser1                     NUMBER (10);
      kuser2                     NUMBER (10); /* Transaction Types constants */
      kcreditnote                NUMBER (10);
      kinvoicewater              NUMBER (10);
      kinvoicesewer              NUMBER (10);
      kinvoiceelec               NUMBER (10);
      kinvoicerefuse             NUMBER (10);
      kinvoiceborehole           NUMBER (10);
      kinvoiceratestax           NUMBER (10);
      kinvoiceuser1              NUMBER (10);
      kinvoiceuser2              NUMBER (10); /* Transaction Sub-types constants */
      kconsumpinvoice            NUMBER (10);
      kminchargesadj             NUMBER (10);
      kwaterestimadj             NUMBER (10);
      ksewerestimadj             NUMBER (10);
      kelecestimadj              NUMBER (10); /* Reading Type constants */
      kreadingactual             NUMBER (10);
      kreadingestimrev           NUMBER (10);
      kreadingestimnonrev        NUMBER (10); /* Currency rounding setting constants */
      nissueroundingdiff         NUMBER (10);
      nrounddown                 NUMBER (10);
      nroundup                   NUMBER (10);
      nroundnearest              NUMBER (10);
      nroundto                   NUMBER (10);
      nnoofdigits                NUMBER (10);
      nroundingmethod            NUMBER (10); /* Date Limits */
      dtveryolddate              DATE;
      dtfarfuturedate            DATE; /* Transaction Category and Ordering */
      order1crnote               NUMBER (10);
      order2consinv              NUMBER (10);
      order3mincharge            NUMBER (10);
      order4regcharge            NUMBER (10); /* Billing Services */
      kwatersrvon                NUMBER (10);
      ksewersrvon                NUMBER (10);
      kelecsrvon                 NUMBER (10);
      krefsrvon                  NUMBER (10);
      kboreholesrvon             NUMBER (10);
      kratestaxsrvon             NUMBER (10);
      kuser1on                   NUMBER (10);
      kuser2on                   NUMBER (10); /* Estimation Option constants */
      bestimnometer              NUMBER (1);
      bestimnonopermeter         NUMBER (1);
      bestimopermeter            NUMBER (1);
      nestimnomonths             NUMBER (10);
      nestimnordgs               NUMBER (10);
      nassessmethod              NUMBER (10); /* Tariff variables */
      kmaxbandno                 NUMBER (10);
      nband                      NUMBER (10);
      knodiscount                NUMBER (10);
      kdiscconsum                NUMBER (10);
      kdiscconsumandminchrg      NUMBER (10);
      nnominchargeforvacated     NUMBER (10);
      nnominchargefordiscon      NUMBER (10); /* Regular charge transaction range */
      kregchargemin              NUMBER (10);
      kregchargemax              NUMBER (10); /* Regular charges calculation variables */
      nchrgyear                  NUMBER (10);
      nchrgmonth                 NUMBER (3);
      nchrgday                   NUMBER (3);
      nchrgdate                  DATE; /* Regular Charge entity constants */
      kmtrentity                 NUMBER (10);
      kconnentity                NUMBER (10);
      kpropentity                NUMBER (10); /* Regular Charge lookup relation constants */
      knorelation                NUMBER (10);
      kuserdefined               NUMBER (10);
      kvariablepertariff         NUMBER (10);
      kvariablepermetersize      NUMBER (10);
      kvariableperconsumertype   NUMBER (10);
      kvariableperconsumpttype   NUMBER (10);
      kvariableperinfoflag1      NUMBER (10);
      kvariableperinfoflag2      NUMBER (10);
      kvariableperinfoflag3      NUMBER (10);
      kvariableperinfoflag4      NUMBER (10);
      kvariableperinfoflag5      NUMBER (10);
      kfixedcharge               NUMBER (10); /* Transaction generation variables */
      nroundingdiff              NUMBER (19, 4);
      ntotalamount               NUMBER (19, 4);
      strcurcustkey              NVARCHAR2 (15);
      ntmptransno                NUMBER (10);
      ntmpdiscount               NUMBER (19, 4);
      dttmpdateto                DATE;
      dteffectdate               DATE;
      nConsumption               NUMBER(20,8); /* 2017-07 */
      nSplitReadingPerCycle      number(10); /* 2020-02 */
      strError                   nvarchar2(128); /* 2020-02 */
   BEGIN
      progress.start_of_process (p_nprocessid, 51);

      /* ===========================================================================
         ===========================================================================
         ====  Assign constants for this stored procedure  =========================
         ===========================================================================
         =========================================================================== */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 1 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      kthisstep := 3;
      kundefinedcycle := 0; /* Cycle Length */

      BEGIN
         SELECT NVL (cycle_len, 1)
           INTO ncyclelengthmonths
           FROM sum_bcyc
          WHERE cycle_id = p_nbillcycleid;
      EXCEPTION
         WHEN OTHERS
         THEN
            ncyclelengthmonths := 1;
      END;

      ncyclelengthdays := ncyclelengthmonths * 365.0 / 12.0;

      IF ((ncyclelengthmonths <= 0) OR (ncyclelengthdays <= 0))
      THEN
         raise_application_error (-20971, 'Invalid Cycle Length');
         RETURN;
      END IF; /* Cycle Cut-off date */

      BEGIN
         SELECT bilng_date
           INTO dtcyclecutoffday
           FROM sum_bcyc
          WHERE cycle_id = p_nbillcycleid;
      EXCEPTION
         WHEN OTHERS
         THEN
            dtcyclecutoffday := NULL;
      END;

      IF dtcyclecutoffday IS NULL
      THEN
         raise_application_error (-20970, 'Missing Cycle Cut-off Date');
         RETURN;
      END IF; /* Billing group  */

      BEGIN
         SELECT billgroup
           INTO strbillgroup
           FROM sum_bcyc
          WHERE cycle_id = p_nbillcycleid;
      EXCEPTION
         WHEN OTHERS
         THEN
            strbillgroup := NULL;
      END;

      IF strbillgroup IS NULL
      THEN
         raise_application_error (-20969, 'Missing Billing Group');
         RETURN;
      END IF; /* Service Numbers */

      kwatersrv := 0;
      ksewersrv := 1;
      kelectsrv := 2;
      krefsrv := 3;
      kboreholesrv := 4;
      kratestaxsrv := 5;
      kuser1 := 6;
      kuser2 := 7; /* Transaction Types */
      kcreditnote := 40;
      kinvoicewater := 60;
      kinvoicesewer := 70;
      kinvoiceelec := 80;
      kinvoicerefuse := 82;
      kinvoiceborehole := 83;
      kinvoiceratestax := 84;
      kinvoiceuser1 := 86;
      kinvoiceuser2 := 88; /* Transaction Sub-types */
      kconsumpinvoice := 2;
      kminchargesadj := 8;
      kwaterestimadj := 11;
      ksewerestimadj := 12;
      kelecestimadj := 13; /* Reading Types */
      kreadingactual := 0;
      kreadingestimrev := 1;
      kreadingestimnonrev := 2; /* Currency rounding settings */

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO nissueroundingdiff
           FROM SETTINGS
          WHERE keyword = 'GEN_SEPERATE_TRANS_FOR_DIFF';
      EXCEPTION
         WHEN OTHERS
         THEN
            nissueroundingdiff := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO nrounddown
           FROM SETTINGS
          WHERE keyword = 'GEN_ROUND_MTHD_TRUNC';
      EXCEPTION
         WHEN OTHERS
         THEN
            nrounddown := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO nroundup
           FROM SETTINGS
          WHERE keyword = 'GEN_ROUND_MTHD_UP';
      EXCEPTION
         WHEN OTHERS
         THEN
            nroundup := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO nroundnearest
           FROM SETTINGS
          WHERE keyword = 'GEN_ROUND_MTHD_NEAREST';
      EXCEPTION
         WHEN OTHERS
         THEN
            nroundnearest := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 1)
           INTO nroundto
           FROM SETTINGS
          WHERE keyword = 'GEN_CURRENCY_ROUNDTO';
      EXCEPTION
         WHEN OTHERS
         THEN
            nroundto := 1;
      END;

      BEGIN
         SELECT NVL (keyword_value, 2)
           INTO nnoofdigits
           FROM SETTINGS
          WHERE keyword = 'GEN_CURRENCY_DIGITS';
      EXCEPTION
         WHEN OTHERS
         THEN
            nnoofdigits := 2;
      END;

      IF (nroundnearest = 1)
      THEN
         nroundingmethod := 1;
      ELSIF (nrounddown = 1)
      THEN
         nroundingmethod := 3;
      ELSIF (nroundup = 1)
      THEN
         nroundingmethod := 2;
      ELSE
         nroundingmethod := 1; /* Round Nearest */
      END IF; /* Date Limits */

      dtveryolddate := TO_DATE ('31-12-1950', 'DD-MM-YYYY');
      dtfarfuturedate := TO_DATE ('31-12-2099', 'DD-MM-YYYY');

      /* Transaction Category and Ordering */
      order1crnote := 1;
      order2consinv := 2;
      order3mincharge := 3;
      order4regcharge := 4;

      /* ===========================================================================
         ===========================================================================
         ====  Create Data Model for relevant entities  ============================
         ===========================================================================
         =========================================================================== */

      /* ---------------------------------------------------------------------------
         Empty all temporary tables used by this stored procedure
        --------------------------------------------------------------------------- */
      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 2 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Tables should be already empty so DELETE should not be expensive */

      DELETE FROM tmp_trans;

      DELETE FROM tmp_props;

      DELETE FROM tmp_prop_owners;

      DELETE FROM tmp_cons;

      DELETE FROM tmp_s_cons;

      DELETE FROM tmp_meters;

      DELETE FROM tmp_metr_rdgs;

      DELETE FROM tmp_metr_rdg_id;

      DELETE FROM tmp_rdgs_estim;

      DELETE FROM tmp_rev_estim;

      DELETE FROM tmp_tariffs;

      DELETE FROM tmp_rdg_tariffs;

      DELETE FROM tmp_rdg_tariff_bands;

      DELETE FROM tmp_regcharge_dates;

      DELETE FROM tmp_reg_charges;


      /* ---------------------------------------------------------------------------
         Create temporary table that will hold only the customers from the specified
         billing group.
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 3 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Table skipped for the time being until there is a bigger need for it */

      /* ---------------------------------------------------------------------------
      Create temporary table that will hold only properties occupied by customers
      from the specified billing group.
      --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 4 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF; /* Reset services that are not activated */

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO kwatersrvon
           FROM SETTINGS
          WHERE keyword = 'GEN_LEDGER1_ACTIVE';
      EXCEPTION
         WHEN OTHERS
         THEN
            kwatersrvon := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO ksewersrvon
           FROM SETTINGS
          WHERE keyword = 'GEN_LEDGER2_ACTIVE';
      EXCEPTION
         WHEN OTHERS
         THEN
            ksewersrvon := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO kelecsrvon
           FROM SETTINGS
          WHERE keyword = 'GEN_LEDGER3_ACTIVE';
      EXCEPTION
         WHEN OTHERS
         THEN
            kelecsrvon := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO krefsrvon
           FROM SETTINGS
          WHERE keyword = 'GEN_LEDGER4_ACTIVE';
      EXCEPTION
         WHEN OTHERS
         THEN
            krefsrvon := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO kboreholesrvon
           FROM SETTINGS
          WHERE keyword = 'GEN_LEDGER5_ACTIVE';
      EXCEPTION
         WHEN OTHERS
         THEN
            kboreholesrvon := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO kratestaxsrvon
           FROM SETTINGS
          WHERE keyword = 'GEN_LEDGER6_ACTIVE';
      EXCEPTION
         WHEN OTHERS
         THEN
            kratestaxsrvon := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO kuser1on
           FROM SETTINGS
          WHERE keyword = 'GEN_LEDGER7_ACTIVE';
      EXCEPTION
         WHEN OTHERS
         THEN
            kuser1on := 0;
      END;

      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO kuser2on
           FROM SETTINGS
          WHERE keyword = 'GEN_LEDGER8_ACTIVE';
      EXCEPTION
         WHEN OTHERS
         THEN
            kuser2on := 0;
      END;

      INSERT INTO tmp_props
         SELECT p.prop_ref, p.bill_custkey AS custkey,
                CASE kwatersrvon
                   WHEN 0
                      THEN 0
                   ELSE (CASE p.srv1_alcto
                            WHEN 1
                               THEN 1
                            WHEN 0
                               THEN 1
                            ELSE 0
                         END)
                END,
                CASE ksewersrvon
                   WHEN 0
                      THEN 0
                   ELSE (CASE p.srv2_alcto
                            WHEN 1
                               THEN 1
                            WHEN 0
                               THEN 1
                            ELSE 0
                         END)
                END,
                CASE kelecsrvon
                   WHEN 0
                      THEN 0
                   ELSE (CASE p.srv3_alcto
                            WHEN 1
                               THEN 1
                            WHEN 0
                               THEN 1
                            ELSE 0
                         END)
                END,
                CASE krefsrvon
                   WHEN 0
                      THEN 0
                   ELSE (CASE p.srv4_alcto
                            WHEN 1
                               THEN 1
                            WHEN 0
                               THEN 1
                            ELSE 0
                         END)
                END,
                CASE kboreholesrvon
                   WHEN 0
                      THEN 0
                   ELSE (CASE p.srv5_alcto
                            WHEN 1
                               THEN 1
                            WHEN 0
                               THEN 1
                            ELSE 0
                         END)
                END,
                CASE kratestaxsrvon
                   WHEN 0
                      THEN 0
                   ELSE (CASE p.srv6_alcto
                            WHEN 1
                               THEN 1
                            WHEN 0
                               THEN 1
                            ELSE 0
                         END)
                END,
                CASE kuser1on
                   WHEN 0
                      THEN 0
                   ELSE (CASE p.srv7_alcto
                            WHEN 1
                               THEN 1
                            WHEN 0
                               THEN 1
                            ELSE 0
                         END)
                END,
                CASE kuser2on
                   WHEN 0
                      THEN 0
                   ELSE (CASE p.srv8_alcto
                            WHEN 1
                               THEN 1
                            WHEN 0
                               THEN 1
                            ELSE 0
                         END)
                END,
                0, p.c_type, p.township_no, p.info_flag1, p.info_flag2,
                p.info_flag3, p.info_flag4, p.info_flag5,
                NVL (p.rfs_nobins, 1) AS rfs_nobins, p.no_rooms,
                NVL (p.is_vacated, 0) AS is_vacated,
                NVL (p.prop_value, 0) AS prop_value
           FROM prop_dtl p, cust_dtl c
          WHERE p.bill_custkey = c.custkey AND c.billgroup = strbillgroup
            and c.CUSTKEY = case when length(p_strCustkey) > 0 then p_strCustkey else c.CUSTKEY end /*2017-11*/;

      /* 2017-11 Remove already billed */
      delete from tmp_props tmp
       where exists (select 1 from F_TRANS ft
                      where tmp.CUSTKEY = ft.CUSTKEY
                        and ft.BILL_CYCLE_ID =  p_nbillcycleid
                        and ft.BILL_CYCLE_STEP = 3);

      /* Remove Properties that have no services activated */
      DELETE FROM tmp_props
            WHERE srv1_alcto = 0
              AND srv2_alcto = 0
              AND srv3_alcto = 0
              AND srv4_alcto = 0
              AND srv5_alcto = 0
              AND srv6_alcto = 0
              AND srv7_alcto = 0
              AND srv8_alcto = 0
              AND srv9_alcto = 0;

      /* Create separate table for property owners for rates system */

      INSERT INTO tmp_prop_owners
         SELECT o.prop_ref, o.custkey, o.share_numerator, o.share_denominator,
                NULL, /* C_TYPE      */ NULL, /* TOWNSHIP_NO */ NULL, /* INFO_FLAG1  */ NULL, /* INFO_FLAG2  */
                NULL, /* INFO_FLAG3  */ NULL, /* INFO_FLAG4  */ NULL, /* INFO_FLAG5  */
                -999 /* PROP_VALUE  */
           FROM prop_owners o, cust_dtl c
          WHERE o.custkey = c.custkey
            AND c.billgroup = strbillgroup
            AND kratestaxsrvon = 1;

      /* Populate table for property owners from property details */

      UPDATE tmp_prop_owners
         SET (c_type, township_no, info_flag1, info_flag2, info_flag3,
              info_flag4, info_flag5, prop_value) =
                (SELECT p.c_type, p.township_no, p.info_flag1, p.info_flag2,
                        p.info_flag3, p.info_flag4, p.info_flag5,
                        NVL (p.prop_value, 0)
                   FROM prop_dtl p
                  WHERE p.prop_ref = tmp_prop_owners.prop_ref
                    AND NVL (p.srv6_alcto, -1) IN (0, 1))
       WHERE kratestaxsrvon = 1
         AND EXISTS (
                SELECT 1
                  FROM prop_dtl p
                 WHERE p.prop_ref = tmp_prop_owners.prop_ref
                   AND NVL (p.srv6_alcto, -1) IN (0, 1));

      /* Remove entries for properties that are not allocated to the Rates Service */

      DELETE FROM tmp_prop_owners
            WHERE prop_value = -999;

      /* --------------------------------------------------------------------------
      Create temporary table that will hold only water & electricity Connections
      related to the above properties.
      --------------------------------------------------------------------------*/

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 5 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      INSERT INTO tmp_cons
                  (prop_ref, conn_no, service, tariff_id, con_status,
                   vol_discon, meter_type, meter_ref, assessed_cons, no_flats,
                   conn_date, sewer_exists, custkey, c_type, township_no,
                   is_vacated, info_flag1, info_flag2, info_flag3, info_flag4,
                   info_flag5,
                   CONN_TYPE, BLK_PROPRF, BLK_CONNNO /* 2017-07 */
                   )
         SELECT c.prop_ref, c.conn_no, NVL (c.serv_catg, 0), NULL,
                NVL (c.con_status, 1), c.vol_discon, c.meter_type,
                c.meter_ref, c.estim_cons,
                CASE NVL (c.no_flats, 1)
                   WHEN 0
                      THEN 1
                   ELSE NVL (c.no_flats, 1)
                END, NVL (c.conn_date, dtveryolddate), 0, p.custkey, p.c_type,
                p.township_no, p.is_vacated, p.info_flag1, p.info_flag2,
                p.info_flag3, p.info_flag4, p.info_flag5,
                nvl(c.CONN_TYPE, 0), c.BLK_PROPRF, c.BLK_CONNNO /* 2017-07 */
           FROM conn_dtl c, tmp_props p
          WHERE NVL (c.conn_date, dtveryolddate) < dtcyclecutoffday
            and nvl(C.CONN_TYPE, 0) <> 3  /* 2017-07 Exclude Check Meter connections */
            AND c.prop_ref = p.prop_ref
            AND (   (NVL (c.serv_catg, 0) = kwatersrv AND p.srv1_alcto = 1)
                 OR (NVL (c.serv_catg, 0) = kelectsrv AND p.srv3_alcto = 1)
                );

      /* For multi-activity connections get no flats and assessed cons */
      update tmp_cons a
         set NO_FLATS = (select sum(CASE NVL (b.NO_UNITS, 1)
                                       WHEN 0
                                       THEN 1
                                       ELSE NVL (b.NO_UNITS, 1)
                                    END)
                           from CONN_DTL_TARIFF_ALLOC b
                          where a.PROP_REF = b.PROP_REF
                            and a.CONN_NO = b.CONN_NO),
             ASSESSED_CONS = (select sum(ESTIM_CONS_PU /  ncyclelengthmonths /**
                                          CASE NVL (b.NO_UNITS, 1)
                                              WHEN 0
                                              THEN 1
                                              ELSE NVL (b.NO_UNITS, 1)
                                          END */)
                                from CONN_DTL_TARIFF_ALLOC b
                               where a.PROP_REF = b.PROP_REF
                                 and a.CONN_NO = b.CONN_NO)
       where exists (select 1
                       from CONN_DTL_TARIFF_ALLOC b
                      where a.PROP_REF = b.PROP_REF
                        and a.CONN_NO = b.CONN_NO);


      /* Update Water & Electricity tariff ID */

      UPDATE tmp_cons c
         SET tariff_id =
                (SELECT CASE c.service
                           WHEN kwatersrv
                              THEN ct.wtariff_id
                           WHEN kelectsrv
                              THEN ct.etariff_id
                           ELSE NULL
                        END
                   FROM ctg_consumptiontypes ct
                  WHERE c.c_type = ct.ctype_id);

      /* --------------------------------------------------------------------------
         Create temporary table that will hold only sewer Connections
         related to the above properties.
         --------------------------------------------------------------------------*/

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 6 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      INSERT INTO tmp_s_cons
                  (prop_ref, conn_no, tariff_id, assessed_scons, conn_date,
                   water_exists, custkey, c_type, township_no, is_vacated,
                   info_flag1, info_flag2, info_flag3, info_flag4, info_flag5)
         SELECT s.prop_ref, s.conn_no, NULL, s.estim_cons,
                NVL (s.conn_date, dtveryolddate), 0, p.custkey, p.c_type,
                p.township_no, p.is_vacated, p.info_flag1, p.info_flag2,
                p.info_flag3, p.info_flag4, p.info_flag5
           FROM sconn_dtl s, tmp_props p
          WHERE s.serv_catg = ksewersrv
            AND s.prop_ref = p.prop_ref
            AND p.srv2_alcto = 1
            AND NVL (s.conn_date, dtveryolddate) < dtcyclecutoffday;

      /* Update sewer tariff ID */
      UPDATE tmp_s_cons
         SET tariff_id = (SELECT ct.stariff_id
                            FROM ctg_consumptiontypes ct
                           WHERE tmp_s_cons.c_type = ct.ctype_id);

      /* Mark Sewer Connections with related Water Connections */
      UPDATE tmp_s_cons s
         SET water_exists = 1
       WHERE conn_no = (SELECT MIN (conn_no)
                          FROM tmp_s_cons g
                         WHERE g.prop_ref = s.prop_ref)
         AND EXISTS (SELECT 1
                       FROM tmp_cons c
                      WHERE c.prop_ref = s.prop_ref AND c.service = kwatersrv);

      /* Update No of Flats from TMP_CONS on first sewer connection only */
      UPDATE tmp_s_cons s
         SET no_flats =
                    (SELECT   SUM (c.no_flats)
                         FROM tmp_cons c
                        WHERE c.prop_ref = s.prop_ref
                              AND c.service = kwatersrv
                     GROUP BY c.prop_ref)
       WHERE water_exists = 1;

      /* --------------------------------------------------------------------------
         Update water connections with information from related
         sewer Connections.
         -------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 7 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Update Water Connections with related Sewer Connection info */

      UPDATE tmp_cons c
         SET (sewer_exists, stariff_id, sconn_date, assessed_scons) =
                (SELECT 1, s.tariff_id, s.conn_date, s.assessed_scons
                   FROM tmp_s_cons s
                  WHERE s.prop_ref = c.prop_ref
                    AND s.water_exists = 1
                    AND c.service = kwatersrv)
       WHERE c.service = kwatersrv
         AND EXISTS (SELECT 1
                       FROM tmp_s_cons sc
                      WHERE sc.prop_ref = c.prop_ref AND sc.water_exists = 1);

      /* --------------------------------------------------------------------------
         Create a temporary table that will hold only meters
         related to the above connections.
         -------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 8 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      INSERT INTO tmp_meters
         SELECT c.meter_type, c.meter_ref,
                CASE NVL (m.op_status, 0)
                   WHEN 1
                      THEN 1
                   ELSE 0
                END, CASE
                   WHEN NVL (m.no_dials, 0) < 2
                      THEN 4
                   ELSE m.no_dials
                END, CASE
                   WHEN NVL (m.rev_flow, 0) <> 1
                      THEN 0
                   ELSE m.rev_flow
                END, CASE NVL (m.conv_fctr, 0)
                   WHEN 0
                      THEN 1
                   ELSE m.conv_fctr
                END, m."SIZE", NULL, c.prop_ref, c.conn_no, c.service,
                c.tariff_id, c.con_status, c.vol_discon, c.estim_cons,
                c.no_flats, c.conn_date, c.sewer_exists, c.stariff_id,
                c.sconn_date, c.custkey, c.c_type, c.is_vacated, c.info_flag1,
                c.info_flag2, c.info_flag3, c.info_flag4, c.info_flag5
           FROM metr_dtl m, tmp_cons c
          WHERE c.meter_type = m.meter_type
            AND c.meter_ref = m.meter_ref
            AND m.status = 1;

      /* Clear METER_REF from TMP_CONS where no such meter exists */
      UPDATE tmp_cons tc
         SET meter_type = NULL,
             meter_ref = NULL
       WHERE NOT EXISTS (
                SELECT 1
                  FROM tmp_meters m
                 WHERE tc.meter_type = m.meter_type
                   AND tc.meter_ref = m.meter_ref);

      /* --------------------------------------------------------------------------
      Get the most recent installation reading - if none then take the
      oldest available reading
      -------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 9 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Empty temporary table (might have been used before) */

      DELETE FROM tmp_metr_rdg_id;

      /* For each meter get most recent installation reading (separate table for speed) */

      INSERT INTO tmp_metr_rdg_id
         SELECT   m.meter_type, m.meter_ref, NULL,
                  MIN (r.reading_no) AS id_reading_no
             FROM vwmetr_rdg r, tmp_meters m
            WHERE r.meter_type = m.meter_type
              AND r.meter_ref = m.meter_ref
              AND r.read_reasn = 1
              AND NVL (r.is_cancelled, 0) = 0
         GROUP BY m.meter_type, m.meter_ref;

      /* Update meters with latest installation reading information */

      UPDATE tmp_meters m
         SET last_inst_read_no =
                (SELECT r.id_reading_no
                   FROM tmp_metr_rdg_id r
                  WHERE r.meter_type = m.meter_type
                    AND r.meter_ref = m.meter_ref);

      /* Empty temporary table (might be re-used later on) */

      DELETE FROM tmp_metr_rdg_id;

      /* For meter where no installation reading exists use the first reading as
         installation reading (separate table for speed) */

      INSERT INTO tmp_metr_rdg_id
         SELECT   m.meter_type, m.meter_ref, NULL,
                  MAX (r.reading_no) AS id_reading_no
             FROM vwmetr_rdg r, tmp_meters m
            WHERE r.meter_type = m.meter_type
              AND r.meter_ref = m.meter_ref
              AND NVL (r.is_cancelled, 0) = 0
              AND m.last_inst_read_no IS NULL
         GROUP BY m.meter_type, m.meter_ref;


      /* Update remaining meters with first reading information */

      UPDATE tmp_meters m
         SET last_inst_read_no =
                (SELECT r.id_reading_no
                   FROM tmp_metr_rdg_id r
                  WHERE r.meter_type = m.meter_type
                    AND r.meter_ref = m.meter_ref)
       WHERE m.last_inst_read_no IS NULL;

      /* Empty temporary table (might be re-used later on) */

      DELETE FROM tmp_metr_rdg_id;

      /* ===========================================================================
         ===========================================================================
         ====  Processing of Actual Meter Readings  ================================
         ===========================================================================
         =========================================================================== */


      /* ---------------------------------------------------------------------------
         Take all the readings after the last installation reading, that have not
         been cancelled and invoiced. All readings from this table (that are not
         invoiced yet) should be invoiced.
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 10 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Insert non-invoiced readings into the temporary table */
      INSERT INTO tmp_metr_rdgs
                  (READ_ID,
                   meter_type, meter_ref, reading_no, service, tariff_id,
                   reading_type, reading, read_date, clock_over, prev_reading,
                   custkey, prop_ref, conn_no, c_type, no_flats, no_dials,
                   rev_flow, conv_fctr, last_inst_read_no)
         SELECT (select coalesce(max(READ_ID),0) from TMP_METR_RDGS) + rownum,
                m.meter_type, m.meter_ref, r.reading_no, m.service,
                m.tariff_id, 'ACTUAL', r.b_reading, r.cur_date, r.clock_over,
                0, m.custkey, m.prop_ref, m.conn_no, m.c_type, m.no_flats,
                m.no_dials, m.rev_flow, m.conv_fctr, m.last_inst_read_no
           FROM tmp_meters m, vwmetr_rdg r
          WHERE m.meter_type = r.meter_type
            AND m.meter_ref = r.meter_ref
            AND r.reading_no < m.last_inst_read_no
            AND NVL (r.is_invoicd, 0) = 0
            AND NVL (r.is_cancelled, 0) = 0
            AND r.cur_date <= dtcyclecutoffday
            AND NVL (r.read_reasn, 0) <> 1;

      /* Exclude installation readings */

      /* ---------------------------------------------------------------------------
         For each invoicable reading get previous reading information (separate table for speed)
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 11 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      INSERT INTO tmp_metr_rdg_id
         SELECT   r.meter_type, r.meter_ref, r.reading_no,
                  MIN (m.reading_no) AS id_reading_no
             FROM vwmetr_rdg m, tmp_metr_rdgs r
            WHERE m.meter_type = r.meter_type
              AND m.meter_ref = r.meter_ref
              AND m.reading_no > r.reading_no
              AND m.reading_no <= r.last_inst_read_no
              AND m.cur_date <= r.read_date
              AND NVL (m.is_cancelled, 0) = 0
         GROUP BY r.meter_type, r.meter_ref, r.reading_no;

      /* Update estimate readings with previous reading information */
      UPDATE tmp_metr_rdgs r
         SET prev_reading_no =
                (SELECT m.id_reading_no
                   FROM tmp_metr_rdg_id m
                  WHERE m.meter_type = r.meter_type
                    AND m.meter_ref = r.meter_ref
                    AND m.reading_no = r.reading_no);

      /* Empty temporary table (might be re-used later on) */
      DELETE FROM tmp_metr_rdg_id;

      /* Remove all readings with no valid previous reading (initial readings - normally none) */
      DELETE FROM tmp_metr_rdgs
            WHERE prev_reading_no IS NULL;

     /* ---------------------------------------------------------------------------
        Calculate consumption for each reading
        --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 12 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Get previous reading info and calculate consumption */
      UPDATE tmp_metr_rdgs tr
         SET (prev_read_date, prev_reading, consumption) =
                (SELECT mr.cur_date, mr.b_reading, tr.reading - mr.b_reading
                   FROM vwmetr_rdg mr
                  WHERE tr.meter_type = mr.meter_type
                    AND tr.meter_ref = mr.meter_ref
                    AND tr.prev_reading_no = mr.reading_no)
       WHERE EXISTS (
                SELECT 1
                  FROM vwmetr_rdg vmr
                 WHERE tr.meter_type = vmr.meter_type
                   AND tr.meter_ref = vmr.meter_ref
                   AND tr.prev_reading_no = vmr.reading_no); /* Adjust for reverse flow */

      UPDATE tmp_metr_rdgs
         SET consumption = 0 - consumption
       WHERE NVL (rev_flow, 0) = 1;

      /* Adjust clockovers */
      UPDATE tmp_metr_rdgs
         SET consumption = POWER (10, no_dials) + consumption
       WHERE consumption < 0 AND NVL (clock_over, 0) = 1;

      /* Delete Negative consumption readings (normally none here) */
      DELETE FROM tmp_metr_rdgs
            WHERE consumption < 0 OR consumption IS NULL;

      /* Adjust for Conversion factor */
      UPDATE tmp_metr_rdgs
         SET consumption = consumption * NVL (conv_fctr, 1)
       WHERE NVL (conv_fctr, 1) <> 1;


     /* 2017-07 Bulk meter adjustment */
     /* ---------------------------------------------------------------------------
        Bulk Meter Adjustments
        --------------------------------------------------------------------------- */

     /* Adjust Consumption for Bulk Meters by subtracting actual consumption of sub-meters */

    for curSubConsump in ( select sc.BLK_PROPRF, sc.BLK_CONNNO,
                                  coalesce(sum(r.CONSUMPTION), 0) AS CONSUMP
                           from tmp_cons sc, TMP_METR_RDGS r, tmp_cons bc
                           where sc.CONN_TYPE = '1'     /* Sub Meter */
                             and sc.PROP_REF = r.PROP_REF
                             and sc.CONN_NO = r.CONN_NO
                             and bc.CONN_TYPE = '2'     /* Bulk Meter */
                             and bc.PROP_REF = sc.BLK_PROPRF
                             and bc.CONN_NO = sc.BLK_CONNNO
                           group by sc.BLK_PROPRF, sc.BLK_CONNNO
                           order by sc.BLK_PROPRF, sc.BLK_CONNNO )
    loop
      nConsumption:= curSubConsump.CONSUMP;

      for curBulkConsump in ( select READ_ID, CONSUMPTION
                              from TMP_METR_RDGS
                              where PROP_REF = curSubConsump.BLK_PROPRF
                                and CONN_NO = curSubConsump.BLK_CONNNO )
      loop
        if nConsumption > 0 then
          if curBulkConsump.CONSUMPTION < nConsumption then

            update TMP_METR_RDGS
            set CONSUMPTION = 0
             where PROP_REF = curSubConsump.BLK_PROPRF
               and CONN_NO = curSubConsump.BLK_CONNNO;

            nConsumption:= nConsumption - curBulkConsump.CONSUMPTION;

          elsif curBulkConsump.CONSUMPTION > nConsumption then

            update TMP_METR_RDGS
            set CONSUMPTION = CONSUMPTION - nConsumption
             where PROP_REF = curSubConsump.BLK_PROPRF
               and CONN_NO = curSubConsump.BLK_CONNNO;


            nConsumption:= 0;

          end if;
        end if;
      end loop;
     end loop;

     /* 2017-07 end Bulk meter adjustment */


      /* Calculate flow */
      UPDATE tmp_metr_rdgs
         SET flow =
                  consumption
                / (CASE TRUNC (read_date - prev_read_date)
                      WHEN -1
                         THEN 1
                      WHEN 0
                         THEN 1
                      ELSE TRUNC (read_date - prev_read_date)
                   END
                  );

      /* ---------------------------------------------------------------------------
         Flag connections that have an actual meter reading to bill
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 13 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      UPDATE tmp_cons tc
         SET reading_type = 'ACTUAL'
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_metr_rdgs r
                 WHERE tc.meter_type = r.meter_type
                   AND tc.meter_ref = r.meter_ref);

      /* Set remaining records to NONE */
      UPDATE tmp_cons
         SET reading_type = 'NONE'
       WHERE NVL (reading_type, 'NONE') <> 'ACTUAL';

      /* ---------------------------------------------------------------------------
         Create a copy reading of all water readings for sewer purposes where a sewer
         connection exists
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 14 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      INSERT INTO tmp_metr_rdgs
                  (
                   read_id,
                   meter_type, meter_ref, reading_no, service, tariff_id,
                   reading_type, reading, read_date, clock_over,
                   prev_reading_no, prev_read_date, prev_reading, consumption,
                   flow, charge_amt, charge_disc, charge_cons, custkey,
                   prop_ref, conn_no, c_type, no_flats, no_dials, rev_flow,
                   conv_fctr, last_inst_read_no)
         SELECT (select coalesce(max(READ_ID),0) from TMP_METR_RDGS) + rownum,
                r.meter_type, r.meter_ref, r.reading_no, ksewersrv,
                m.stariff_id, r.reading_type, r.reading, r.read_date,
                r.clock_over, r.prev_reading_no, r.prev_read_date,
                r.prev_reading, r.consumption, r.flow, r.charge_amt,
                r.charge_disc, r.charge_cons, r.custkey, r.prop_ref,
                r.conn_no, r.c_type, r.no_flats, r.no_dials, r.rev_flow,
                r.conv_fctr, r.last_inst_read_no
           FROM tmp_metr_rdgs r, tmp_meters m
          WHERE m.meter_type = r.meter_type
            AND m.meter_ref = r.meter_ref
            AND m.service = kwatersrv
            AND m.sewer_exists = 1;


    /* 2020-02 */
    /* -------------------------------------------------------------------------------------------------------------------------- */

    /* ---------------------------------------------------------------------------------/
     Split reading per period if the setting is active and reading spans more periods
       than the cycle length
     ----------------------------------------------------------------------------------- */

    begin
      select NVL(KEYWORD_VALUE, 0) into nSplitReadingPerCycle
      from SETTINGS
      where KEYWORD = 'BIL_SETTINGS_SplitReadingPerCycle';
    exception
      when others then
        nSplitReadingPerCycle := 0;
    end;

    if nSplitReadingPerCycle = 1 then

     declare
              /* Cursor vars */
         nReadID      number(10);
         dReadDate    date;
         dPrvReadDate date;
         nFlow        number(20,8);
         nConsump     number(20,8);
         nReading     number(20,8);
         nPrevReading number(20,8);
         nDials       number(10);
         nRevFlow     number(10);
         nConvFactor  number(20,8);

         /* working vars */
         nCnt            number(10);
         dNewReadDate    date;
         dNewPrvReadDate date;
         nCurrConsump    number(20,8);
         nTotConsump     number(20,8);
         nNewReading     number(20,8);
         nNewPrevReading number(20,8);
         nDoNotSplit     number(10);
         nClocked        number(10);

     begin

         /* Identify long period readings */
         for cur_rdgs in (
         select READ_ID, READ_DATE, PREV_READ_DATE, FLOW, CONSUMPTION,
                READING, PREV_READING, NO_DIALS, REV_FLOW, CONV_FCTR
           from TMP_METR_RDGS a
          where MONTHS_BETWEEN(READ_DATE, PREV_READ_DATE) > nCycleLengthMonths
            and LINK_READ_ID is null
            and not exists (select 1 from CHARGES_REGULAR z
                             where a.TARIFF_ID = z.TARIFF_ID
                               and z.PROJECTION = 0
                               and z.IGNORE_TIMEFFECT = 0 /* Applied only for tarriffs with ignored time effect */
                               and z.EFF_DATE >= (select max(z1.EFF_DATE) from CHARGES_REGULAR z1
                                                   where z.TARIFF_ID = z1.TARIFF_ID
                                                     and z.PROJECTION =  z1.PROJECTION
                                                     and z1.EFF_DATE <= a.PREV_READ_DATE) )
          order by READ_ID)
         loop

           /* Festch cursor values */
           nReadID      := cur_rdgs.READ_ID;
           dReadDate    := cur_rdgs.READ_DATE;
           dPrvReadDate := cur_rdgs.PREV_READ_DATE;
           nFlow        := cur_rdgs.FLOW;
           nConsump     := cur_rdgs.CONSUMPTION;
           nReading     := cur_rdgs.READING;
           nPrevReading := cur_rdgs.PREV_READING;
           nDials       := cur_rdgs.NO_DIALS;
           nRevFlow     := cur_rdgs.REV_FLOW;
           nConvFactor  := cur_rdgs.CONV_FCTR;

           /* Init variables per reading loop */
           dNewPrvReadDate := null;
           nCurrConsump    := null;
           nTotConsump     := null;
           nNewPrevReading := null;

           nCnt := 1;
           dNewReadDate := dReadDate;
           nTotConsump := nConsump;
           nNewReading := nReading;
           nDoNotSplit := 0;

           while dNewReadDate <> dPrvReadDate and nDoNotSplit = 0
           loop

                nClocked := 0;

                /* Set the reading dates */
                dNewPrvReadDate := ADD_MONTHS(dNewReadDate,(-1 * nCycleLengthMonths));
                if dNewPrvReadDate < dPrvReadDate then
                  dNewPrvReadDate := dPrvReadDate;
                end if;

                /* Apportion the consumption and calculate the running consumption remaining */
                if (nTotConsump >= 0 and nRevFlow = 0) or (nTotConsump <= 0 and nRevFlow = 1) then
                     if (dNewPrvReadDate = dPrvReadDate) then
                       nCurrConsump := nTotConsump;
                     else
                       nCurrConsump := round(nFlow * (dNewReadDate - dNewPrvReadDate),0);
                     end if;

                     /* If we exceeded the total then adjust */
                     if (nTotConsump - nCurrConsump >= 0 and nRevFlow = 0) or (nTotConsump - nCurrConsump <= 0 and nRevFlow = 1) then
                       nTotConsump := nTotConsump - nCurrConsump;
                     else
                       nCurrConsump := nTotConsump;
                       nTotConsump := 0;
                     end  if;
                else
                     nTotConsump  := 0;
                     nCurrConsump := 0;
                end if;

                if nCurrConsump = 0 and nCnt = 1 then /* Consumption to low to split */
                  nDoNotSplit := 1;
                else
                 begin

                    /*if no more consumption will be split then set the reading date to orig prev read date so we do not loop more */
                    if nTotConsump = 0 then
                     dNewPrvReadDate := dPrvReadDate;
                    end if;

                    /* Set the readings */
                    if nNewReading -
                        ((case when nRevFlow = 1 then 0 - nCurrConsump else nCurrConsump end)
                           /coalesce(nConvFactor, 1))
                       >= 0 then

                      nNewPrevReading := nNewReading -
                                             ((case when nRevFlow = 1 then 0 - nCurrConsump else nCurrConsump end)
                                              /coalesce(nConvFactor, 1));

                    else

                     nClocked := 1;
                     nNewPrevReading := POWER(10, nDials)
                                            - ((case when nRevFlow = 1 then 0 - nCurrConsump else nCurrConsump end)/coalesce(nConvFactor, 1))
                                            + nNewReading;
                    end if;

                    /* Insert split record */
                    insert into TMP_METR_RDGS
                     (
                     METER_TYPE, METER_REF, READING_NO, SERVICE, TARIFF_ID, READING_TYPE,
                     READING, READ_DATE, CLOCK_OVER,
                     PREV_READING_NO, PREV_READ_DATE, PREV_READING,
                     CONSUMPTION,
                     FLOW,
                     CHARGE_AMT, CHARGE_DISC, CHARGE_CONS,
                     CUSTKEY, PROP_REF, CONN_NO,
                     C_TYPE, NO_FLATS, NO_DIALS, REV_FLOW, CONV_FCTR,
                     LAST_INST_READ_NO,
                     LINK_READ_ID, SUB_PERIOD
                     )
                    select  METER_TYPE, METER_REF, READING_NO,  SERVICE, TARIFF_ID, READING_TYPE,
                            nNewReading, dNewReadDate, nClocked,
                            PREV_READING_NO, dNewPrvReadDate, nNewPrevReading,
                            nCurrConsump,
                            nCurrConsump/(CASE dNewReadDate - dNewPrvReadDate
                                             WHEN -1 THEN 1
                                             WHEN 0 THEN 1
                                             ELSE dNewReadDate - dNewPrvReadDate
                                          END) as FLOW /* Set the flow accordingly */,
                            CHARGE_AMT, CHARGE_DISC, CHARGE_CONS,
                            CUSTKEY, PROP_REF, CONN_NO,
                            C_TYPE, NO_FLATS, NO_DIALS, REV_FLOW, CONV_FCTR,
                            LAST_INST_READ_NO,
                            nReadID, nCnt
                    from TMP_METR_RDGS
                    where READ_ID = nReadID;

                    /* Set dates and cnt for next loop */
                    dNewReadDate := dNewPrvReadDate;
                    nNewReading := nNewPrevReading;
                    nCnt := nCnt + 1;

                 end;
                end if;

           end loop;

           /* Clear the split reading so that it is not charged */
           if  nDoNotSplit = 0 then

              /* Check consumption and date reconcile */
              for cur_rdgs in
               (
               select sum(case when LINK_READ_ID is null then CONSUMPTION else 0 end),
                      sum(case when LINK_READ_ID is not null then CONSUMPTION else 0 end) as SPLIT_SUM,
                      max(case when LINK_READ_ID is null then READ_DATE else trunc(sysdate) end),
                      max(case when LINK_READ_ID is not null then READ_DATE else trunc(sysdate) end),
                      min(case when LINK_READ_ID is null then PREV_READ_DATE else trunc(sysdate) end),
                      min(case when LINK_READ_ID is not null then PREV_READ_DATE else trunc(sysdate) end)
                 from TMP_METR_RDGS
                where (READ_ID = nReadID) or (LINK_READ_ID = nReadID)
               having (sum(case when LINK_READ_ID is null then CONSUMPTION else 0 end) <>
                       sum(case when LINK_READ_ID is not null then CONSUMPTION else 0 end))
                   or (max(case when LINK_READ_ID is null then READ_DATE else trunc(sysdate) end) <>
                       max(case when LINK_READ_ID is not null then READ_DATE else trunc(sysdate) end))
                   or (min(case when LINK_READ_ID is null then PREV_READ_DATE else trunc(sysdate) end) <>
                       min(case when LINK_READ_ID is not null then PREV_READ_DATE else trunc(sysdate) end))
                )
              loop
               strError:= 'Split Consumption calculation not correct ' || cast(nReadID as nvarchar2) || ' ';
               raise_application_error( -20946, strError );
               return;
              end loop;

              update TMP_METR_RDGS
              set CONSUMPTION = 0,
                  FLOW = 0
              where READ_ID = nReadID;

           end if;

         end loop;

     end;

    end if;

    ---------------------------------------------------------------------------------------------------------------------------



      /* ===========================================================================
         ===========================================================================
         ====  Generation of Estimated Readings  ===================================
         ===========================================================================
         =========================================================================== */

      /* ---------------------------------------------------------------------------
         Flag connections for estimates & assessment
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 15 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Flag connections with no meter for assessment  */
      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO bestimnometer
           FROM SETTINGS
          WHERE keyword = 'BIL_STATEMENTS_Option1';
      EXCEPTION
         WHEN OTHERS
         THEN
            bestimnometer := 0;
      END;

      IF (bestimnometer = 1)
      THEN
         UPDATE tmp_cons
            SET reading_type = 'ASSESS'
          WHERE reading_type = 'NONE'
            AND meter_type IS NULL
            AND meter_ref IS NULL
            AND NVL (con_status, 0) NOT IN
                                   (2, 3) /* Connection is not disconnected */
            AND NVL (is_vacated, 0) <> 1; /* Property is not vacated        */
      END IF;

      /* Flag connections with non-operational meter for estimates */
      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO bestimnonopermeter
           FROM SETTINGS
          WHERE keyword = 'BIL_STATEMENTS_Option2';
      EXCEPTION
         WHEN OTHERS
         THEN
            bestimnonopermeter := 0;
      END;

      IF (bestimnonopermeter = 1)
      THEN
         UPDATE tmp_cons c
            SET reading_type = 'ESTIM_NOP'
          WHERE c.reading_type = 'NONE'
            AND NVL (c.con_status, 0) NOT IN
                                   (2, 3) /* Connection is not disconnected */
            AND NVL (c.is_vacated, 0) <> 1 /* Property is not vacated        */
            AND EXISTS (
                   SELECT 1
                     FROM tmp_meters m
                    WHERE c.meter_type = m.meter_type
                      AND c.meter_ref = m.meter_ref
                      AND NVL (m.op_status, 0) = 1); /* Meter non-operational          */
      END IF;

      /* Flag connections with operational meter for estimates */
      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO bestimopermeter
           FROM SETTINGS
          WHERE keyword = 'BIL_STATEMENTS_Option3';
      EXCEPTION
         WHEN OTHERS
         THEN
            bestimopermeter := 0;
      END;

      IF (bestimopermeter = 1)
      THEN
         UPDATE tmp_cons c
            SET reading_type = 'ESTIM_OP'
          WHERE c.reading_type = 'NONE'
            AND NVL (c.con_status, 0) NOT IN
                                   (2, 3) /* Connection is not disconnected */
            AND NVL (c.is_vacated, 0) <> 1 /* Property is not vacated        */
            AND EXISTS (
                   SELECT 1
                     FROM tmp_meters m
                    WHERE c.meter_type = m.meter_type
                      AND c.meter_ref = m.meter_ref
                      AND NVL (m.op_status, 0) <> 1); /* Meter operational              */
      END IF;

      /* 2017-07 Reset estimation for Bulk Meter Connections */
      update TMP_CONS
         set READING_TYPE = 'NONE'
       where CONN_TYPE = '2'   /* Bulk Meter */
         and READING_TYPE in ('ASSESS','ESTIM_OP','ESTIM_NOP');

      /* ---------------------------------------------------------------------------
         Extract readings that can be used for estimation into separate table
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 16 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      BEGIN
         SELECT NVL (keyword_value, 6)
           INTO nestimnomonths
           FROM SETTINGS
          WHERE keyword = 'BIL_STATEMENTS_NoMonths';
      EXCEPTION
         WHEN OTHERS
         THEN
            nestimnomonths := 6;
      END;

      /* Move qualifying readings into separate table */
      INSERT INTO tmp_rdgs_estim
         SELECT r.meter_type, r.meter_ref, r.reading_no, r.b_reading,
                r.cur_date, r.clock_over, NULL, NULL, NULL, NULL, NULL,
                m.no_dials, m.rev_flow, m.conv_fctr, m.last_inst_read_no
           FROM tmp_cons c, tmp_meters m, vwmetr_rdg r
          WHERE c.reading_type IN ('ESTIM_OP', 'ESTIM_NOP')
            AND c.meter_type = m.meter_type
            AND c.meter_ref = m.meter_ref
            AND c.meter_type = r.meter_type
            AND c.meter_ref = r.meter_ref
            AND m.last_inst_read_no > r.reading_no
            AND NVL (r.is_cancelled, 0) <> 1
            AND r.cur_date <= dtcyclecutoffday
            AND TRUNC (dtcyclecutoffday - r.cur_date) <=
                                                 nestimnomonths * 365.0 / 12.0;

      /* ---------------------------------------------------------------------------
         Process readings that will be used for estimation
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 17 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Delete records where not sufficient readings are available - first run */
      BEGIN
         SELECT NVL (keyword_value, 3)
           INTO nestimnordgs
           FROM SETTINGS
          WHERE keyword = 'BIL_STATEMENTS_NoReadings';
      EXCEPTION
         WHEN OTHERS
         THEN
            nestimnordgs := 3;
      END;

      DELETE FROM tmp_rdgs_estim t
            WHERE EXISTS (
                     SELECT   meter_type, meter_ref, COUNT (1) AS rec_no
                         FROM tmp_rdgs_estim e
                        WHERE e.meter_type = t.meter_type
                          AND e.meter_ref = t.meter_ref
                     GROUP BY meter_type, meter_ref
                       HAVING COUNT (1) < nestimnordgs);

      /* For each invoicable reading get previous reading information (separate table for speed) */
      INSERT INTO tmp_metr_rdg_id
         SELECT   e.meter_type, e.meter_ref, e.reading_no,
                  MIN (m.reading_no) AS id_reading_no
             FROM vwmetr_rdg m, tmp_rdgs_estim e
            WHERE m.meter_type = e.meter_type
              AND m.meter_ref = e.meter_ref
              AND m.reading_no > e.reading_no
              AND m.reading_no <= e.last_inst_read_no
              AND m.cur_date <= e.read_date
              AND NVL (m.is_cancelled, 0) = 0
         GROUP BY e.meter_type, e.meter_ref, e.reading_no;

      /* Update estimate readings with previous reading information */
      UPDATE tmp_rdgs_estim e
         SET prev_reading_no =
                (SELECT id_reading_no
                   FROM tmp_metr_rdg_id m
                  WHERE m.meter_type = e.meter_type
                    AND m.meter_ref = e.meter_ref
                    AND m.reading_no = e.reading_no);

      /* Empty temporary table (might be re-used later on) */
      DELETE FROM tmp_metr_rdg_id;

      /* Remove all readings with no valid previous reading */
      DELETE FROM tmp_rdgs_estim
            WHERE prev_reading_no IS NULL;

      /* ---------------------------------------------------------------------------
         Calculate historical consumption using applicable readings
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 18 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Get previous reading info and calculate consumption */
      UPDATE tmp_rdgs_estim tr
         SET (prev_read_date, prev_reading, consumption) =
                (SELECT mr.cur_date, mr.b_reading, tr.reading - mr.b_reading
                   FROM vwmetr_rdg mr
                  WHERE tr.meter_type = mr.meter_type
                    AND tr.meter_ref = mr.meter_ref
                    AND tr.prev_reading_no = mr.reading_no)
       WHERE EXISTS (
                SELECT 1
                  FROM vwmetr_rdg m
                 WHERE tr.meter_type = m.meter_type
                   AND tr.meter_ref = m.meter_ref
                   AND tr.prev_reading_no = m.reading_no);

      /* Update Meter Information from TMP_METERS */
      UPDATE tmp_rdgs_estim r
         SET (rev_flow, conv_fctr) =
                (SELECT NVL (m.rev_flow, 0), NVL (m.conv_fctr, 1)
                   FROM tmp_meters m
                  WHERE r.meter_type = m.meter_type
                    AND r.meter_ref = m.meter_ref)
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_meters tm
                 WHERE r.meter_type = tm.meter_type
                   AND r.meter_ref = tm.meter_ref);

      /* Adjust for reverse flow */
      UPDATE tmp_rdgs_estim
         SET consumption = 0 - consumption
       WHERE NVL (rev_flow, 0) = 1;

      /* Adjust clockovers */
      UPDATE tmp_rdgs_estim
         SET consumption = POWER (10, no_dials) + consumption
       WHERE consumption < 0 AND NVL (clock_over, 0) = 1;

      /* Delete Negative consumption readings (normally none here) */
      DELETE FROM tmp_rdgs_estim
            WHERE consumption < 0 OR consumption IS NULL;

      /* Adjust for Conversion factor */
      UPDATE tmp_rdgs_estim
         SET consumption = consumption * NVL (conv_fctr, 1)
       WHERE NVL (conv_fctr, 1) <> 1;

      /* Delete records where not sufficient readings are available - second run */
      DELETE FROM tmp_rdgs_estim te
            WHERE EXISTS (
                     SELECT   meter_type, meter_ref, COUNT (1) AS rec_no
                         FROM tmp_rdgs_estim e
                        WHERE e.meter_type = te.meter_type
                          AND e.meter_ref = te.meter_ref
                     GROUP BY meter_type, meter_ref
                       HAVING COUNT (1) < nestimnordgs);

      /* ---------------------------------------------------------------------------
         Calculate cycle consumption for estimated readings
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 19 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Calculate the number of days covered by estimated readings */
      UPDATE tmp_cons
         SET estim_days =
                (SELECT SUM (read_date - prev_read_date)
                   FROM tmp_rdgs_estim e
                  WHERE e.meter_type = tmp_cons.meter_type
                    AND e.meter_ref = tmp_cons.meter_ref)
       WHERE reading_type IN ('ESTIM_OP', 'ESTIM_NOP');

      /* In the unlikely event that we have 0 days change to 1 to avoid division overflow */
      UPDATE tmp_cons
         SET estim_days = 1
       WHERE reading_type IN ('ESTIM_OP', 'ESTIM_NOP')
         AND estim_days IS NOT NULL
         AND estim_days <= 0;

      /* Calculate estimated daily flow by summing consumption and dividing by no of days */
      UPDATE tmp_cons
         SET estim_flow =
                (SELECT SUM (consumption) / estim_days
                   FROM tmp_rdgs_estim e
                  WHERE e.meter_type = tmp_cons.meter_type
                    AND e.meter_ref = tmp_cons.meter_ref)
       WHERE reading_type IN ('ESTIM_OP', 'ESTIM_NOP')
         AND estim_days IS NOT NULL;

      /* Mark very low estimated consumption as NULL so that they can be assessed later on */
      UPDATE tmp_cons
         SET estim_flow = NULL,
             estim_days = NULL
       WHERE reading_type IN ('ESTIM_OP', 'ESTIM_NOP')
         AND estim_flow IS NOT NULL
         AND estim_flow * ncyclelengthdays < 1; /* Less than 1kl / cycle */

      /* Copy estimated daily flow to sewer consumption where relevant */
      UPDATE tmp_cons
         SET estim_s_flow = estim_flow
       WHERE reading_type IN ('ESTIM_OP', 'ESTIM_NOP')
         AND estim_flow IS NOT NULL
         AND service = kwatersrv
         AND sewer_exists = 1;

      /* Reset number of days to be assigned later on based on the cycle length */
      UPDATE tmp_cons
         SET estim_days = NULL
       WHERE reading_type IN ('ESTIM_OP', 'ESTIM_NOP')
         AND estim_days IS NOT NULL;

      /* ---------------------------------------------------------------------------
         Calculate cycle consumption for assessed connections (including connections
         where no estimate reading could be calculated)
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 20 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Calculate assessments based on method */
      BEGIN
         SELECT NVL (keyword_value, 0)
           INTO nassessmethod
           FROM SETTINGS
          WHERE keyword = 'BIL_STATEMENTS_AssessmentMethod';
      EXCEPTION
         WHEN OTHERS
         THEN
            nassessmethod := 0;
      END;

      /* Assign assessments by Consumption category */
      IF (nassessmethod = 1)
      THEN

         /* Assign to water and electricity connections where there is no individual assessment */
         UPDATE tmp_cons
            SET assessed_cons =
                     no_flats
                   * (SELECT CASE tmp_cons.service
                                WHEN kwatersrv
                                   THEN ct.w_assessed_cons
                                WHEN kelectsrv
                                   THEN ct.e_assessed_cons
                                ELSE NULL
                             END
                        FROM ctg_consumptiontypes ct
                       WHERE ct.ctype_id = tmp_cons.c_type)
          WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
            AND NVL (assessed_cons, 0) < 1; /* Exclude Individual assessments */
      END IF;

      /* Assign assessments by Township */
      IF (nassessmethod = 2)
      THEN

         /* Assign to water and electricity connections where there is no individual assessment */
         UPDATE tmp_cons
            SET assessed_cons =
                     no_flats
                   * (SELECT CASE tmp_cons.service
                                WHEN kwatersrv
                                   THEN twn.w_assessed_cons
                                WHEN kelectsrv
                                   THEN twn.e_assessed_cons
                                ELSE NULL
                             END
                        FROM township_dtl twn
                       WHERE twn.township_no = tmp_cons.township_no)
          WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
            AND NVL (assessed_cons, 0) < 1; /* Exclude Individual assessments */
      END IF;

      /* Copy assessed water consumption to sewer where there is no individual assessment */
      UPDATE tmp_cons
         SET assessed_scons = assessed_cons
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND service = kwatersrv
         AND sewer_exists = 1
         AND NVL (assessed_scons, 0) < 1; /* Exclude Individual assessments */

      /* Calculate daily assessed consumption for connections that do not have estimates */
      UPDATE tmp_cons
         SET estim_flow =
                  assessed_cons
                * 12
                / 365 /* Assessed consumption is kept monthly */
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_flow IS NULL; /* Exclude actual estimates */

      /* Calculate daily assessed consumption for sewer connections that do not have estimates */
      UPDATE tmp_cons
         SET estim_s_flow =
                  assessed_scons
                * 12
                / 365 /* Assessed consumption is kept monthly */
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_s_flow IS NULL /* Exclude actual estimates */
         AND service = kwatersrv
         AND sewer_exists = 1;

      /* ---------------------------------------------------------------------------
         Calculate the period for which estimates apply based on the cycle length
         and each connections connection date
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 21 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Calculate the number of days for assessment based on the cycle length */
      UPDATE tmp_cons
         SET estim_days = ncyclelengthdays
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_flow IS NOT NULL;

      /* Calculate the number of days for sewer assessment based on the cycle length */
      UPDATE tmp_cons
         SET estim_s_days = ncyclelengthdays
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_s_flow IS NOT NULL;

      /* Adjust the number of days for assessment based on the connection date */
      UPDATE tmp_cons
         SET estim_days = TRUNC (dtcyclecutoffday - conn_date)
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_flow IS NOT NULL
         AND estim_days > TRUNC (dtcyclecutoffday - conn_date);

      /* Adjust the number of days for assessment based on the sewer connection date */
      UPDATE tmp_cons
         SET estim_s_days = TRUNC (dtcyclecutoffday - sconn_date)
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_s_flow IS NOT NULL
         AND estim_s_days > TRUNC (dtcyclecutoffday - sconn_date);

      /* Normally there should not be any negative no of days as all connections
         that were connected after the billing cut-off date have been filtered out
         when building the SP data model*/

      /* Multiply daily consumption by number of days and round number of days */
      UPDATE tmp_cons
         SET estim_cons = TRUNC (estim_flow * estim_days + 0.5),
             estim_days = TRUNC (estim_days + 0.5)
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_flow IS NOT NULL;

      /* Recalculate water/electricity flow based on the rounded figures */
      UPDATE tmp_cons
         SET estim_flow = estim_cons / estim_days
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_flow IS NOT NULL;

      /* Multiply daily sewer consumption by number of days and round number of days */
      UPDATE tmp_cons
         SET estim_s_cons = TRUNC (estim_s_flow * estim_s_days + 0.5),
             estim_s_days = TRUNC (estim_s_days + 0.5)
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_s_flow IS NOT NULL;

      /* Recalculate sewer flow based on the rounded figures */
      UPDATE tmp_cons
         SET estim_s_flow = estim_s_cons / estim_s_days
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_s_flow IS NOT NULL;

      /* Reset sewer estimation if sewer consumption is very small */
      UPDATE tmp_cons
         SET estim_s_flow = NULL,
             estim_s_days = NULL,
             estim_s_cons = NULL
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_s_cons IS NOT NULL
         AND estim_s_cons < 1; /* Also takes care of negative ESTIM_S_DAYS */

      /* Reset estimation if consumption is very small */
      UPDATE tmp_cons
         SET reading_type = 'NONE',
             estim_flow = NULL,
             estim_days = NULL,
             estim_cons = NULL,
             estim_s_flow = NULL,
             estim_s_days = NULL,
             estim_s_cons = NULL
       WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
         AND estim_cons IS NOT NULL
         AND estim_cons < 1; /* Also takes care of negative ESTIM_DAYS */

      /* ---------------------------------------------------------------------------
      Insert dummy readings to emulate estimation / assessment calculation
      --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 22 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Update Meter Reference for assessed connections with no meter */
      UPDATE tmp_cons
         SET meter_type = 'E',
             meter_ref = 'ASSESSED'
       WHERE reading_type = 'ASSESS'
         AND meter_type IS NULL
         AND meter_ref IS NULL;

      /* Readings for assessed/estimated Water/Electricity consumption */
      INSERT INTO tmp_metr_rdgs
                  (meter_type, meter_ref, reading_no, service, tariff_id,
                   reading_type, reading, read_date, clock_over,
                   prev_reading_no, prev_read_date, prev_reading, consumption,
                   flow, charge_amt, charge_disc, charge_cons, custkey,
                   prop_ref, conn_no, c_type, no_flats, no_dials, rev_flow,
                   conv_fctr, last_inst_read_no)
         SELECT meter_type, meter_ref, 0, service, tariff_id, reading_type,
                estim_cons, dtcyclecutoffday, 0, NULL,
                dtcyclecutoffday - estim_days, 0, estim_cons, estim_flow,
                NULL, NULL, NULL, custkey, prop_ref, conn_no, c_type,
                no_flats, 8, 0, 1, NULL
           FROM tmp_cons
          WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
            AND estim_cons >= 1;

      /* Readings for assessed/estimated Sewer consumption */
      INSERT INTO tmp_metr_rdgs
                  (meter_type, meter_ref, reading_no, service, tariff_id,
                   reading_type, reading, read_date, clock_over,
                   prev_reading_no, prev_read_date, prev_reading, consumption,
                   flow, charge_amt, charge_disc, charge_cons, custkey,
                   prop_ref, conn_no, c_type, no_flats, no_dials, rev_flow,
                   conv_fctr, last_inst_read_no)
         SELECT meter_type, meter_ref, 0, ksewersrv, stariff_id, reading_type,
                estim_s_cons, dtcyclecutoffday, 0, NULL,
                dtcyclecutoffday - estim_s_days, 0, estim_s_cons,
                estim_s_flow, NULL, NULL, NULL, custkey, prop_ref, conn_no,
                c_type, no_flats, 8, 0, 1, NULL
           FROM tmp_cons
          WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
            AND estim_s_cons >= 1
            AND service = kwatersrv
            AND sewer_exists = 1;

      /* ---------------------------------------------------------------------------
         Insert dummy readings for individually assessed sewer connections
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 23 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Identify qualifying records and calculate daily flow */
      UPDATE tmp_s_cons
         SET estim_s_flow = assessed_scons * 12 / 365.0,
             estim_s_days = ncyclelengthdays
       WHERE water_exists = 1 AND NVL (assessed_scons, 0) > 1; /* Value exists */

      /* Adjust the number of days for assessment based on the sewer connection date */
      UPDATE tmp_s_cons
         SET estim_s_days = TRUNC (dtcyclecutoffday - conn_date)
       WHERE estim_s_flow IS NOT NULL
         AND estim_s_days > TRUNC (dtcyclecutoffday - conn_date);

      /* Normally there should not be any negative no of days as all connections
         that were connected after the billing cut-off date have been filtered out
         when building the SP data model  */

      /* Multiply daily sewer consumption by number of days and round number of days */
      UPDATE tmp_s_cons
         SET estim_s_cons = TRUNC (estim_s_flow * estim_s_days + 0.5),
             estim_s_days = TRUNC (estim_s_days + 0.5)
       WHERE estim_s_flow IS NOT NULL;

      /* Recalculate sewer flow based on the rounded figures */
      UPDATE tmp_s_cons
         SET estim_s_flow = estim_s_cons / estim_s_days
       WHERE estim_s_flow IS NOT NULL;

      /* Reset sewer estimation if sewer consumption is very small */
      UPDATE tmp_s_cons
         SET estim_s_flow = NULL,
             estim_s_days = NULL,
             estim_s_cons = NULL
       WHERE estim_s_flow IS NOT NULL AND estim_s_cons < 1; /* Also takes care of negative ESTIM_S_DAYS */

      /* ---------------------------------------------------------------------------
      Insert dummy readings to emulate sewer assessment calculation
      --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 24 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Readings for assessed/estimated Water/Electricity consumption */
      INSERT INTO tmp_metr_rdgs
                  (meter_type, meter_ref, reading_no, service, tariff_id,
                   reading_type, reading, read_date, clock_over,
                   prev_reading_no, prev_read_date, prev_reading, consumption,
                   flow, charge_amt, charge_disc, charge_cons, custkey,
                   prop_ref, conn_no, c_type, no_flats, no_dials, rev_flow,
                   conv_fctr, last_inst_read_no)
         SELECT 'E', 'ASSESSED', 0, ksewersrv, tariff_id, 'ASSESS',
                estim_s_cons, dtcyclecutoffday, 0, NULL,
                dtcyclecutoffday - estim_s_days, 0, estim_s_cons,
                estim_s_flow, NULL, NULL, NULL, custkey, prop_ref, conn_no,
                c_type, no_flats, NULL, 0, 1, NULL
           FROM tmp_s_cons
          WHERE estim_s_flow IS NOT NULL AND estim_s_cons >= 1;

      /*******************************************************************************************/
      /* 2017-07
      /* ACTIVITIES: 1.Generate one record here per activity. Make Sure to allocated Tariff IDS  */
      /*              In tmp_metr_Rdgs table . Use sewr perc for portioning                      */
      /*******************************************************************************************/

         UPDATE tmp_metr_rdgs SET read_id = ROWNUM; /* 2017-07 */

         /* Water */
         INSERT INTO tmp_metr_rdgs /* 2017-07 */
               (read_Id, meter_type, meter_ref, reading_no, service,
                tariff_id,
                reading_type, reading, read_date, clock_over,
                prev_reading_no, prev_read_date, prev_reading,
                consumption,
                flow,
                charge_amt, charge_disc, charge_cons, custkey,
                prop_ref, conn_no,
                c_type,
                no_flats,
                no_dials, rev_flow,
                conv_fctr, last_inst_read_no, ALLOC_PERC, SAME_READ_ID)
         SELECT READ_ID,
                a.meter_type, a.meter_ref, a.reading_no, a.service,
                d.TARIFF_ID,
                a.reading_type, a.reading, a.read_date, a.clock_over,
                a.prev_reading_no, a.prev_read_date, a.prev_reading,
                case when a.reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP') then b.ESTIM_CONS_PU else a.consumption * round((b.ALLOC_PERC/100),5) end,
                case when a.reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP') then b.ESTIM_CONS_PU else a.consumption * round((b.ALLOC_PERC/100),5) end
                / (case trunc (a.read_date - a.prev_read_date)
                      when -1
                         then 1
                      when 0
                         then 1
                      else trunc (a.read_date - a.prev_read_date)
                   end
                  )  as flow,
                a.charge_amt, a.charge_disc, a.charge_cons, a.custkey,
                a.prop_ref, a.conn_no,
                b.c_type,
                CASE NVL (b.NO_UNITS, 1)
                      WHEN 0
                      THEN 1
                      ELSE NVL (b.NO_UNITS, 1)
                END,
                a.no_dials, a.rev_flow,
                a.conv_fctr, a.last_inst_read_no, b.ALLOC_PERC, a.READ_ID
           FROM TMP_METR_RDGS a,
                CONN_DTL_TARIFF_ALLOC b,
                CTG_CONSUMPTIONTYPES c,
                CHARGES_REGULAR d
          where a.PROP_REF = b.PROP_REF
            and a.CONN_NO = b.CONN_NO
            and b.C_TYPE = c.CTYPE_ID
            and c.WTARIFF_ID = d.TARIFF_ID
            and d.SRV_CATG = 0
            and a.SERVICE = 0
            and b.ALLOC_PERC <> 0;

        /* Sewer */
         INSERT INTO tmp_metr_rdgs /* 2017-07 */
               (read_Id, meter_type, meter_ref, reading_no, service,
                tariff_id,
                reading_type, reading, read_date, clock_over,
                prev_reading_no, prev_read_date, prev_reading,
                consumption,
                flow,
                charge_amt, charge_disc, charge_cons, custkey,
                prop_ref, conn_no,
                c_type,
                no_flats,
                no_dials, rev_flow,
                conv_fctr, last_inst_read_no, ALLOC_PERC, SAME_READ_ID)
         SELECT READ_ID,
                a.meter_type, a.meter_ref, a.reading_no, a.service,
                d.TARIFF_ID,
                a.reading_type, a.reading, a.read_date, a.clock_over,
                a.prev_reading_no, a.prev_read_date, a.prev_reading,
                case when a.reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP') then b.ESTIM_CONS_PU else a.consumption * round((b.ALLOC_PERC/100),5) end,
                case when a.reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP') then b.ESTIM_CONS_PU else a.consumption * round((b.ALLOC_PERC/100),5) end
                / (case trunc (a.read_date - a.prev_read_date)
                      when -1
                         then 1
                      when 0
                         then 1
                      else trunc (a.read_date - a.prev_read_date)
                   end
                  )  as flow,
                a.charge_amt, a.charge_disc, a.charge_cons, a.custkey,
                a.prop_ref, a.conn_no,
                b.c_type,
                CASE NVL (b.NO_UNITS, 1)
                      WHEN 0
                      THEN 1
                      ELSE NVL (b.NO_UNITS, 1)
                END,
                a.no_dials, a.rev_flow,
                a.conv_fctr, a.last_inst_read_no, b.ALLOC_PERC_SEWER, a.READ_ID
           FROM TMP_METR_RDGS a,
                CONN_DTL_TARIFF_ALLOC b,
                CTG_CONSUMPTIONTYPES c,
                CHARGES_REGULAR d
          where a.PROP_REF = b.PROP_REF
            and a.CONN_NO = b.CONN_NO
            and b.C_TYPE = c.CTYPE_ID
            and c.STARIFF_ID = d.TARIFF_ID
            and d.SRV_CATG = 1
            and a.SERVICE = 1
            and b.ALLOC_PERC_SEWER <> 0;

        /* 2017-07 Check allocated percentages ok - very important */
        BEGIN
         select count(1) into ncount
           from (SELECT READ_ID, sum(ALLOC_PERC) as COUNT
                   FROM tmp_metr_rdgs r
                  where ALLOC_PERC is not null
                  GROUP by READ_ID
                 having sum(ALLOC_PERC) <> 100) a;
        EXCEPTION
         WHEN OTHERS
         THEN
            ncount := 0;
        END;

         ----------- MODIFY EGYPT 2020
        /*IF ncount <> 0
        THEN
         raise_application_error (-20969, 'Activity Allocations not consistant');
         RETURN;
        END IF;*/

        /* 2017-07 Mark the main reading as well */
        update tmp_metr_rdgs a
           set SAME_READ_ID = READ_ID
         where exists (select 1 from tmp_metr_rdgs b
                        where b.SAME_READ_ID = a.READ_ID);

        /* 2017-07 Make Read ids unique */
        UPDATE tmp_metr_rdgs SET read_id = ROWNUM;

        /* 2017-07 Make sure no rounding difference exists */

        /*update tmp_metr_rdgs a
           set consumption = consumption + (select b.consumption - sum(c.consumption)
                                              from tmp_metr_rdgs b,
                                                   tmp_metr_rdgs c
                                             where b.SAME_READ_ID = a.SAME_READ_ID
                                               and c.SAME_READ_ID = b.SAME_READ_ID
                                               and c.ALLOC_PERC is not null
                                               and b.ALLOC_PERC is null
                                           --- group by b.consumption)
         where a.READ_ID = (select min(z.READ_ID) from tmp_metr_rdgs z
                             where a.SAME_READ_ID = z.SAME_READ_ID
                               and z.SAME_READ_ID is not null
                               and z.ALLOC_PERC is not null);*/

        ---- EGYPT 2020-05

        update tmp_metr_rdgs a
           set consumption = consumption + (select b.consumption - sum(c.consumption)
                                              from tmp_metr_rdgs b,
                                                   tmp_metr_rdgs c
                                             where b.SAME_READ_ID = a.SAME_READ_ID
                                               and c.SAME_READ_ID = b.SAME_READ_ID
                                               and c.ALLOC_PERC is not null
                                               and b.ALLOC_PERC is null
                                             group by  b.SAME_READ_ID, b.consumption)
         where  nvl(a.service,0)=0 -- check for water only because sewer may be not equal 100%
          and a.READ_ID = (select min(z.READ_ID) from tmp_metr_rdgs z
                             where a.SAME_READ_ID = z.SAME_READ_ID
                               and z.SAME_READ_ID is not null
                               and z.ALLOC_PERC is not null);


        /* 2017-07 Check allocated consumption ok - very important */

        /*
        BEGIN
         select count(1) into ncount
           from (select b.consumption - sum(c.consumption)
                   from tmp_metr_rdgs b,
                        tmp_metr_rdgs c
                  where c.SAME_READ_ID = b.SAME_READ_ID
                    and c.SAME_READ_ID is not null
                    and b.SAME_READ_ID is not null
                    and b.ALLOC_PERC is null
                    and c.ALLOC_PERC is not null
                  group by b.consumption
                 having b.consumption - sum(c.consumption) <> 0
                 ) a;
        EXCEPTION
         WHEN OTHERS
         THEN
            ncount := 0;
        END;
        */


        --------EGYPT 2020-05

        BEGIN
         select count(1) into ncount
           from (select b.consumption - sum(c.consumption)
                   from tmp_metr_rdgs b,
                        tmp_metr_rdgs c
                  where c.SAME_READ_ID = b.SAME_READ_ID
                    and c.SAME_READ_ID is not null
                    and b.SAME_READ_ID is not null
                    and b.ALLOC_PERC is null
                    and c.ALLOC_PERC is not null
                    and nvl(b.service,0)=0 -- check for water only because sewer may be not equal 100%
                  group by b.SAME_READ_ID, b.consumption
                 having round( b.consumption - sum(c.consumption),1) <> 0
                 ) a;
        EXCEPTION
         WHEN OTHERS
         THEN
            ncount := 0;
        END;

        IF ncount <> 0
        THEN
         commit;
         raise_application_error (-20970, 'Activity Allocations consumption not consistant' || ncount );
         RETURN;
        END IF;

        /* end of changes 2017-07 */


      /* ===========================================================================
         ===========================================================================
         ====  Reversal of older Estimates when an actual reading is available  ====
         ===========================================================================
         =========================================================================== */


      /* ---------------------------------------------------------------------------
         Identify all reversible transactions for estimates, related to connections
         that have an actual reading so that they can be reversed
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 25 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      INSERT INTO tmp_rev_estim
         SELECT DISTINCT f.custkey, f.trans_no, f.statm_no, f.trns_type,
                         f.trns_stype, f.amount, f.discount, f.prop_ref,
                         f.conn_no, f.meter_type, f.meter_ref, f.c_type,
                         f.tariff_id
                    FROM f_trans f, tmp_cons c
                   WHERE c.reading_type = 'ACTUAL'
                     AND f.billgroup = strbillgroup
                     AND f.custkey = c.custkey
                     AND f.prop_ref = c.prop_ref
                     AND f.conn_no = c.conn_no
                     AND f.meter_type = c.meter_type
                     AND f.meter_ref = c.meter_ref
                     AND f.read_type = kreadingestimrev
                     AND (   (    f.trns_type = kinvoicewater
                              AND c.service = kwatersrv
                             )
                          OR (    f.trns_type = kinvoicesewer
                              AND c.service = kwatersrv
                              AND c.sewer_exists = 1
                             )
                          OR (    f.trns_type = kinvoiceelec
                              AND c.service = kelectsrv
                             )
                         )
                     AND f.trns_stype = kconsumpinvoice
                     AND NVL (f.rev_cycle_id, kundefinedcycle) =
                                                               kundefinedcycle;

      /* ---------------------------------------------------------------------------
         Mark corresponding transactions for minimum charges adjustments
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 26 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      INSERT INTO tmp_rev_estim
                  (custkey, trans_no, statm_no, trns_type, trns_stype, amount,
                   discount, prop_ref, conn_no, meter_type, meter_ref, c_type,
                   tariff_id)
         SELECT DISTINCT f.custkey, f.trans_no, f.statm_no, f.trns_type,
                         f.trns_stype, f.amount, f.discount, f.prop_ref,
                         f.conn_no, f.meter_type, f.meter_ref, f.c_type,
                         f.tariff_id
                    FROM f_trans f, tmp_rev_estim e
                   WHERE f.billgroup = strbillgroup
                     AND f.custkey = e.custkey
                     AND f.statm_no = e.statm_no
                     AND f.prop_ref = e.prop_ref
                     AND f.conn_no = e.conn_no
                     AND f.meter_type = e.meter_type
                     AND f.meter_ref = e.meter_ref
                     AND f.trns_type = e.trns_type
                     AND f.trns_stype = kminchargesadj
                     AND NVL (f.rev_cycle_id, kundefinedcycle) =
                                                               kundefinedcycle;

      /* ---------------------------------------------------------------------------
         Mark corresponding water / electricity connections with reversal totals
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 27 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;


      /*
      UPDATE tmp_cons tc
         SET rev_cycles =
                (SELECT r.rev_cycles
                   FROM (SELECT   custkey, trns_type, prop_ref, conn_no,
                                  meter_type, meter_ref,
                                  COUNT (DISTINCT statm_no) AS rev_cycles
                             FROM tmp_rev_estim
                            WHERE trns_type <> kinvoicesewer
                         GROUP BY custkey,
                                  trns_type,
                                  prop_ref,
                                  conn_no,
                                  meter_type,
                                  meter_ref) r
                  WHERE tc.custkey = r.custkey
                    AND tc.prop_ref = r.prop_ref
                    AND tc.conn_no = r.conn_no
                    AND tc.meter_type = r.meter_type
                    AND tc.meter_ref = r.meter_ref
                    AND (   (    tc.service = kwatersrv
                             AND r.trns_type = kinvoicewater
                            )
                         OR (    tc.service = kelectsrv
                             AND r.trns_type = kinvoiceelec
                            )
                        ));
      */

      UPDATE tmp_cons tc
         SET rev_cycles =
                (SELECT sum(t.CYCLE_LEN) /* r.rev_cycles */
                   FROM (SELECT   distinct a.custkey, a.trns_type, a.prop_ref, a.conn_no,
                                  a.meter_type, a.meter_ref, b.bill_cycle_id
                                  /* COUNT (DISTINCT statm_no) AS rev_cycles */
                             FROM tmp_rev_estim a, f_trans b
                            where a.CUSTKEY = b.CUSTKEY
                              and a.TRANS_NO = b.TRANS_NO
                              and a.trns_type <> kinvoicesewer
                        /* GROUP BY a.custkey,
                                  a.trns_type,
                                  a.prop_ref,
                                  a.conn_no,
                                  a.meter_type,
                                  a.meter_ref*/) r, sum_bcyc t
                  WHERE r.bill_cycle_id = t.cycle_id
                    AND tc.custkey = r.custkey
                    AND tc.prop_ref = r.prop_ref
                    AND tc.conn_no = r.conn_no
                    AND tc.meter_type = r.meter_type
                    AND tc.meter_ref = r.meter_ref
                    AND (   (    tc.service = kwatersrv
                             AND r.trns_type = kinvoicewater
                            )
                         OR (    tc.service = kelectsrv
                             AND r.trns_type = kinvoiceelec
                            )
                        ));

      /* ---------------------------------------------------------------------------
         Mark corresponding sewer connections with reversal totals
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 28 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;


      /*
      UPDATE tmp_s_cons tsc
         SET rev_cycles =
                (SELECT r.rev_cycles
                   FROM (SELECT   custkey, prop_ref,
                                  COUNT (DISTINCT statm_no) rev_cycles
                             FROM tmp_rev_estim
                            WHERE trns_type = kinvoicesewer
                         GROUP BY custkey, prop_ref) r
                  WHERE tsc.custkey = r.custkey
                    AND tsc.prop_ref = r.prop_ref
                    AND tsc.water_exists = 1); */

       UPDATE tmp_s_cons tsc
         SET rev_cycles =
                (SELECT sum(t.CYCLE_LEN) /*r.rev_cycles*/
                   FROM (SELECT   distinct a.custkey, a.prop_ref, b.bill_cycle_id
                                  /* COUNT (DISTINCT statm_no) rev_cycles*/
                             FROM tmp_rev_estim a, f_trans b
                            where a.CUSTKEY = b.CUSTKEY
                              and a.TRANS_NO = b.TRANS_NO
                              and a.trns_type = kinvoicesewer
                         /*GROUP BY custkey, prop_ref */) r,  sum_bcyc t
                  WHERE r.bill_cycle_id = t.cycle_id
                    AND tsc.custkey = r.custkey
                    AND tsc.prop_ref = r.prop_ref
                    AND tsc.water_exists = 1);

      /* ---------------------------------------------------------------------------
         Create Credit Notes for all reversed transactions
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 29 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Sum up all reversed charges & minimum charges into a credit note per meter */
      INSERT INTO tmp_trans
                  (trans_id, custkey, trans_order, trns_type, trns_stype,
                   amount, discount, prop_ref, conn_no, meter_type, meter_ref,
                   c_type, tariff_id, no_units)
         SELECT   NULL, custkey, order1crnote, kcreditnote,
                  trns_type /* Indicates Service */, 0 - SUM (amount),
                  0 - SUM (discount), prop_ref, conn_no, meter_type,
                  meter_ref, MIN (c_type), MIN (tariff_id), COUNT (1)
             FROM tmp_rev_estim
         GROUP BY custkey, trns_type, prop_ref, conn_no, meter_type,
                  meter_ref
           HAVING SUM (amount) <> 0 OR SUM (discount) <> 0;

      /* Assign the proper transaction type based on service */
      UPDATE tmp_trans
         SET trns_stype =
                CASE trns_stype
                   WHEN kinvoicewater
                      THEN kwaterestimadj
                   WHEN kinvoicesewer
                      THEN ksewerestimadj
                   WHEN kinvoiceelec
                      THEN kelecestimadj
                   ELSE NULL
                END
       WHERE trans_order = order1crnote;

      /* ===========================================================================
         ===========================================================================
         ====  Generation of Consumption Charges  ==================================
         ===========================================================================
         =========================================================================== */

      /* ---------------------------------------------------------------------------
         Setup Tariffs information
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 30 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Create temporary table for tariffs for the purpose of having end date */
      INSERT INTO tmp_tariffs
         SELECT tariff_id, eff_date, NULL, NVL (ignore_timeffect, 0),
                NVL (discount_mode, 0), NVL (discount_perc, 0),
                NVL (bands_method, 0), NVL (sewr_perc, 0),
                NVL (is_mincharge, 0), NVL (mincharge_day, 31), NULL,
                NVL (min_charge_op_meter, 0), min_charge_nonop_meter
           FROM charges_regular
          WHERE projection = 0;

      /* Update end date with the next available tariff effective day (if any) */
      UPDATE tmp_tariffs tt
         SET end_date =
                (SELECT   MIN (c.eff_date) - 1
                     FROM charges_regular c
                    WHERE c.tariff_id = tt.tariff_id
                      AND c.eff_date > tt.start_date
                      AND c.projection = 0
                 GROUP BY c.tariff_id);

      /* Set end date for latest tariffs to something big */
      UPDATE tmp_tariffs
         SET end_date = dtfarfuturedate
       WHERE end_date IS NULL;

      /* Safeguard - Set minimum charge for non-operational meters same as for
         operational meters where NULL */
      UPDATE tmp_tariffs
         SET mincharge_nop = mincharge_op
       WHERE mincharge_nop IS NULL;

     /* Safeguard - Set minimum charge day to 1 for small values */
      UPDATE tmp_tariffs
         SET mincharge_day = 1
       WHERE mincharge_day < 1;

     /* Safeguard - Set minimum charge day to last day of month for big values */
      UPDATE tmp_tariffs
         SET mincharge_day =
                CASE
                   WHEN TO_NUMBER (TO_CHAR (dtcyclecutoffday, 'MM')) IN
                                                      (1, 3, 5, 7, 8, 10, 12)
                      THEN 31
                   WHEN TO_NUMBER (TO_CHAR (dtcyclecutoffday, 'MM')) IN
                                                                (4, 6, 9, 11)
                      THEN 30
                   WHEN TO_NUMBER (TO_CHAR (dtcyclecutoffday, 'MM')) = 2
                      THEN 28
                   ELSE 28
                END
       WHERE (    mincharge_day > 31
              AND TO_NUMBER (TO_CHAR (dtcyclecutoffday, 'MM')) IN
                                                      (1, 3, 5, 7, 8, 10, 12)
             )
          OR (    mincharge_day > 30
              AND TO_NUMBER (TO_CHAR (dtcyclecutoffday, 'MM')) IN
                                                                (4, 6, 9, 11)
             )
          OR (    mincharge_day > 28
              AND TO_NUMBER (TO_CHAR (dtcyclecutoffday, 'MM')) = 2
             );

      /* Assign the minimum charge date based on min charge day and the cycle cut-off date */
      UPDATE tmp_tariffs
         SET mincharge_date =
                TRUNC (  dtcyclecutoffday
                       + mincharge_day
                       - TO_NUMBER (TO_CHAR (dtcyclecutoffday, 'DD'))
                      );

      /* Decrement month if the min charge day is after the cut-off date */
      UPDATE tmp_tariffs
         SET mincharge_date =
                CASE
                   WHEN TO_NUMBER (TO_CHAR (mincharge_date, 'MM')) IN
                                                      (1, 3, 5, 7, 8, 10, 12)
                      THEN TRUNC (mincharge_date - 31)
                   WHEN TO_NUMBER (TO_CHAR (mincharge_date, 'MM')) IN
                                                                (4, 6, 9, 11)
                      THEN TRUNC (mincharge_date - 30)
                   WHEN TO_NUMBER (TO_CHAR (mincharge_date, 'MM')) = 2
                      THEN TRUNC (mincharge_date - 28)
                END
       WHERE dtcyclecutoffday < mincharge_date;

      /* ---------------------------------------------------------------------------
         For every reading to be invoiced, get the list of all the tariffs that are
         going to take part into the calculation (can be multiple based on the tariff
         effective date.
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 31 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Assign an ID to each Meter reading to simplify linking */
      UPDATE tmp_metr_rdgs
         SET read_id = ROWNUM;

      /* Create new table from combination of Readings and tariffs */
      INSERT INTO tmp_rdg_tariffs
                (
                  READ_ID, SERVICE, TARIFF_ID, EFF_DATE,
                  IGNORE_TIMEFFECT, BANDS_METHOD, SEWR_PERC,
                  FROM_DATE,
                  "TO_DATE",
                  CONSUMPTION,FLOW,NO_FLATS,NO_DAYS,CHARGE_AMT,CHARGE_DISC,CHARGE_CONS,
                  ALLOC_PERC, /* 2017-07 */
                  SAME_READ_ID  /* 2017-07 */
                )
         SELECT r.read_id, r.service, r.tariff_id, t.start_date,
                t.ignore_timeffect, t.bands_method, NVL (t.sewr_perc, 0),
                CASE
                   WHEN t.start_date < r.prev_read_date
                      THEN r.prev_read_date
                   ELSE t.start_date
                END,
                CASE
                   WHEN t.end_date > r.read_date
                      THEN r.read_date
                   ELSE t.end_date
                END,
                r.consumption, r.flow, r.no_flats, NULL, NULL, NULL, NULL,
                r.ALLOC_PERC,
                r.SAME_READ_ID
           FROM tmp_metr_rdgs r, tmp_tariffs t
          WHERE t.tariff_id = r.tariff_id
            AND (   (    t.ignore_timeffect =
                                      0 /* One reading to many tariffs match */
                     AND t.start_date <= r.read_date
                     AND t.end_date > r.prev_read_date
                    )
                 OR (    t.ignore_timeffect =
                                          1 /* One-to-one match on read date */
                     AND t.start_date <= r.read_date
                     AND t.end_date >= r.read_date
                    )
                );

      COMMIT;

      /* Check if every reading is linked to a tariff - very important */
      BEGIN
         SELECT NVL (COUNT (1), 0)
           INTO ncount
           FROM tmp_metr_rdgs r
          WHERE NOT EXISTS (SELECT 1
                              FROM tmp_rdg_tariffs rt
                             WHERE rt.read_id = r.read_id);
      EXCEPTION
         WHEN OTHERS
         THEN
            ncount := 0;
      END;

      IF ncount <> 0
      THEN
         raise_application_error (-20969, 'Missing Tariff Information');
         RETURN;
      END IF;

      /* ---------------------------------------------------------------------------
         Calculate the daily flow and number of days for each tariff segment
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 32 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      UPDATE tmp_rdg_tariffs
         SET no_days = TO_DATE - from_date;

      /* Exception for same day readings */
      UPDATE tmp_rdg_tariffs
         SET no_days = 1
       WHERE no_days < 1.0;

      /* When time effect is ignored the flow is calculated based on the cycle length */
      UPDATE tmp_rdg_tariffs
         SET flow = consumption / ncyclelengthdays,
             no_days = ncyclelengthdays
       WHERE ignore_timeffect = 1;

      /* For multiple flats divide the flow based on the number of flats */
      UPDATE tmp_rdg_tariffs
         SET flow = flow / no_flats
       WHERE no_flats > 1;

      /* For Sewer readings take into account the % reduction in water flow */
      UPDATE tmp_rdg_tariffs
         SET flow = flow * sewr_perc / 100.0
       WHERE service = ksewersrv AND sewr_perc <> 100;

      /* ---------------------------------------------------------------------------
         Break down each reading-tariff record to a new table per band
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 33 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      kmaxbandno := 9;
      nband := 1;

      WHILE nband <= kmaxbandno
      LOOP
         strsql :=
               'INSERT INTO TMP_RDG_TARIFF_BANDS(
        READ_ID,
        TARIFF_ID,
        EFF_DATE,
        BAND,
        BAND_CHARGE,
        BAND_CHARGE_SQR,
        BAND_CHARGE_CONST,
        BAND_FLOW_FROM,
        BAND_FLOW_TO)
      SELECT RT.READ_ID,
             RT.TARIFF_ID,
             RT.EFF_DATE,
             '
            || nband
            || ', /* maybe redundant */
             T.BND'
            || nband
            || '_CHRG,
             T.BND'
            || nband
            || '_CHRG_SQR,
             T.BND'
            || nband
            || '_CHRG_CNST,
             T.BND'
            || nband
            || '_FROM * 12.0 / nvl(T.BANDS_NOMONTHS,1) / 365.0,
             T.BND'
            || nband
            || '_TO   * 12.0 / nvl(T.BANDS_NOMONTHS, 1) / 365.0
      FROM TMP_RDG_TARIFFS RT, CHARGES_REGULAR T
      WHERE T.TARIFF_ID = RT.TARIFF_ID
        AND T.EFF_DATE = RT.EFF_DATE
        AND T.PROJECTION = 0
        AND T.NO_BANDS >= '
            || nband
            || '
        AND T.BND'
            || nband
            || '_FROM * 12.0 / nvl(T.BANDS_NOMONTHS, 1) / 365.0 < RT.FLOW';

         EXECUTE IMMEDIATE strsql;

         nband := nband + 1;
      END LOOP;

      /* For each reading sub-tariff band, calculate the amount of flow within it. */
      UPDATE tmp_rdg_tariff_bands trb
         SET flow_in_band =
                (SELECT CASE
                           WHEN rt.flow >= trb.band_flow_to
                              THEN trb.band_flow_to - trb.band_flow_from
                           WHEN rt.flow > trb.band_flow_from
                           AND rt.flow < trb.band_flow_to
                              THEN rt.flow - trb.band_flow_from
                           ELSE 0
                        END
                   FROM tmp_rdg_tariffs rt
                  WHERE trb.read_id = rt.read_id
                    AND trb.tariff_id = rt.tariff_id
                    AND trb.eff_date = rt.eff_date);

      /* Calculate amount and consumption for each reading sub-tariff band */
      UPDATE tmp_rdg_tariff_bands trb
         SET (charge_amt, charge_cons) =
                (SELECT CASE rt.bands_method
                           WHEN 0
                              THEN band_charge * flow_in_band * rt.no_days
                           WHEN 1
                              THEN   (    band_charge_sqr
                                        * flow_in_band
                                        * flow_in_band
                                      + band_charge * flow_in_band )
                                   * rt.no_days
                                      + band_charge_const

                           ELSE 0
                        END,
                        flow_in_band * rt.no_days
                   FROM tmp_rdg_tariffs rt
                  WHERE trb.read_id = rt.read_id
                    AND trb.tariff_id = rt.tariff_id
                    AND trb.eff_date = rt.eff_date);

     /* Total-up amount for all reading sub-tariffs into reading tariff */
      UPDATE tmp_rdg_tariffs trt
         SET (charge_amt, charge_cons) =
                (SELECT st.trf_amount, st.trf_consum
                   FROM (SELECT   read_id, tariff_id, eff_date,
                                  SUM (charge_amt) AS trf_amount,
                                  SUM (charge_cons) AS trf_consum
                             FROM tmp_rdg_tariff_bands
                             where flow_in_band>0.00000001---showehdey
                         GROUP BY read_id, tariff_id, eff_date) st
                  WHERE trt.read_id = st.read_id
                    AND trt.tariff_id = st.tariff_id
                    AND trt.eff_date = st.eff_date);

      /* Special case for zero consumption readings */
      UPDATE tmp_rdg_tariffs
         SET charge_amt = 0,
             charge_cons = 0
       WHERE charge_amt IS NULL AND charge_cons IS NULL
             AND consumption < 0.0001;

      /* Check if every tariff / reading is assigned a charge - very important */
      BEGIN
         SELECT NVL (COUNT (1), 0)
           INTO ncount
           FROM tmp_rdg_tariffs r
          WHERE charge_amt IS NULL OR charge_cons IS NULL;
      EXCEPTION
         WHEN OTHERS
         THEN
            ncount := 0;
      END;

      IF ncount <> 0
      THEN
         raise_application_error (-20969, 'Missing Tariff Information');
         RETURN;
      END IF;

      /* For multiple flats multiply amount & consumption based on the number of flats */
      UPDATE tmp_rdg_tariffs
         SET charge_amt = charge_amt * no_flats,
             charge_cons = charge_cons * no_flats
       WHERE no_flats > 1;

      /* ---------------------------------------------------------------------------
         Calculate COD charges for applicable sewer readings per tariff
         --------------------------------------------------------------------------- */

      /*  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      |                                                 |
      |   Postponed - To be implemented for Swaziland   |
      |                                                 |
      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  */

      /* ---------------------------------------------------------------------------
         Calculate discount for each reading tariff
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 34 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      knodiscount := 0;
      kdiscconsum := 1;
      kdiscconsumandminchrg := 2;

      UPDATE tmp_rdg_tariffs trt
         SET charge_disc =
                (SELECT CASE t.discount_mode
                           WHEN knodiscount
                              THEN 0
                           WHEN kdiscconsum
                              THEN charge_amt * t.discount_perc / 100.0
                           WHEN kdiscconsumandminchrg
                              THEN charge_amt * t.discount_perc / 100.0
                           ELSE 0
                        END
                   FROM tmp_tariffs t
                  WHERE t.tariff_id = trt.tariff_id
                    AND t.start_date = trt.eff_date);

      /* Exception when discount is negative (invalid case) */
      UPDATE tmp_rdg_tariffs
         SET charge_disc = 0
       WHERE charge_disc < 0;

      /* Deduct discount from amount */
      UPDATE tmp_rdg_tariffs
         SET charge_amt = charge_amt - charge_disc
       WHERE charge_disc > 0;

      /* ---------------------------------------------------------------------------
         Calculate amount, discount and consumption per reading
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 35 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      UPDATE tmp_metr_rdgs tmr
         SET (charge_amt, charge_disc, charge_cons) =
                (SELECT rt.tot_amt, rt.tot_disc, rt.tot_cons
                   FROM (SELECT   read_id, SUM (charge_amt) AS tot_amt,
                                  SUM (charge_disc) AS tot_disc,
                                  SUM (charge_cons) AS tot_cons
                             FROM tmp_rdg_tariffs
                         GROUP BY read_id) rt
                  WHERE tmr.read_id = rt.read_id);

      /* Normally CONSUMPTION & CONSUMPTION_CHARGED should be the same here except for
         sewer readings where consumption is calculated as a percentage of water consumption */

     /* Delete estimated readings where amount & discount are zero */
      DELETE FROM tmp_metr_rdgs
            WHERE reading_type IN ('ASSESS', 'ESTIM_OP', 'ESTIM_NOP')
              AND charge_amt < 0.0001
              AND charge_disc < 0.0001;

      /* ---------------------------------------------------------------------------
         Calculate amount, discount and consumption per connection
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 36 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Process water / electricity connections */
      UPDATE tmp_cons tc
         SET (charge_amt, charge_disc, charge_cons) =
                (SELECT r.tot_amt, r.tot_disc, r.tot_cons
                   FROM (SELECT   prop_ref, conn_no, service,
                                  SUM (charge_amt) AS tot_amt,
                                  SUM (charge_disc) AS tot_disc,
                                  SUM (charge_cons) AS tot_cons
                             FROM tmp_metr_rdgs
                            WHERE service IN (kwatersrv, ksewersrv)
                         GROUP BY prop_ref, conn_no, service) r
                  WHERE tc.prop_ref = r.prop_ref
                    AND tc.conn_no = r.conn_no
                    AND tc.service = r.service);

      /* Process sewer connections */
      UPDATE tmp_s_cons
         SET (charge_amt, charge_disc, charge_cons) =
                (SELECT r.tot_amt, r.tot_disc, r.tot_cons
                   FROM (SELECT   prop_ref, SUM (charge_amt) AS tot_amt,
                                  SUM (charge_disc) AS tot_disc,
                                  SUM (charge_cons) AS tot_cons
                             FROM tmp_metr_rdgs
                            WHERE service = ksewersrv
                         GROUP BY prop_ref) r
                  WHERE tmp_s_cons.prop_ref = r.prop_ref
                    AND tmp_s_cons.water_exists = 1);

      /* ---------------------------------------------------------------------------
         Insert a transaction per reading to be invoiced
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 37 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /*****JOHN 2013/02/13**************************************************************************/
      /* Identify Cancelled readings in-between current and previous reading that effect min charge */

      FOR CanCelCur IN (select a.READ_ID, sum(s.CYCLE_LEN) as CYCLE_LEN
                          from (select distinct b1.READ_ID, UPD_CYCLE_ID as CYCLE_ID
                                  from VWMETR_RDG a, TMP_METR_RDGS b1
                                 where a.METER_TYPE = b1.METER_TYPE
                                   and a.METER_REF = b1.METER_REF
                                   and a.READING_NO between b1.READING_NO and b1.PREV_READING_NO
                                   and NVL (a.is_cancelled, 0) = 1
                                   and a.IS_INVOICD = 1
                                   and a.UPD_CYCLE_ID > 0
                                   and b1.READING_TYPE = unistr('\0041\0043\0054\0055\0041\004C')
                                   and a.UPD_CYCLE_ID > (select UPD_CYCLE_ID from VWMETR_RDG a1
                                                          where a.METER_TYPE = a1.METER_TYPE
                                                            and a.METER_REF = a1.METER_REF
                                                            and a1.READING_NO = b1.PREV_READING_NO)
                              )  a, SUM_BCYC s
                         where a.CYCLE_ID = s.CYCLE_ID
                         group by a.READ_ID)
      LOOP

        update TMP_METR_RDGS
           set REV_CYCLES = CanCelCur.CYCLE_LEN
         where READ_ID = CanCelCur.READ_ID;

      END LOOP;


      /* Update conns tables with these values as to be considered for min charge */
      update TMP_CONS tc
         set tc.REV_CYCLES = nvl(tc.REV_CYCLES,0)
                                           + (select nvl(r.REV_CYCLES,0)
                                                from TMP_METR_RDGS r
                                               where tc.meter_type = r.meter_type
                                                 and tc.meter_ref = r.meter_ref
                                                 and r.REV_CYCLES > 0
                                                 and r.SERVICE = kwatersrv)
       where exists
                 (select 1
                    from TMP_METR_RDGS r
                   where tc.meter_type = r.meter_type
                     and tc.meter_ref = r.meter_ref
                     and r.REV_CYCLES > 0
                     and r.SERVICE = kwatersrv);

      update TMP_S_CONS tc
         set tc.REV_CYCLES = nvl(tc.REV_CYCLES,0)
                                           + (select nvl(sum(r.REV_CYCLES),0)
                                                from TMP_METR_RDGS r
                                               where tc.PROP_REF = r.PROP_REF
                                                 and r.REV_CYCLES > 0
                                                 and r.SERVICE = ksewersrv)
       where exists
                 (select 1
                    from TMP_METR_RDGS r
                   where tc.PROP_REF = r.PROP_REF
                     and r.REV_CYCLES > 0
                     and r.SERVICE = ksewersrv)
         and tc.water_exists = 1;
      /****************************************************************************/

      INSERT INTO tmp_trans
                  (trans_id, custkey, trans_order, trns_type, trns_stype,
                   amount, discount, prop_ref, conn_no, meter_type, meter_ref,
                   c_type, tariff_id, read_type, reading, prev_reading,
                   read_date, prev_read_date, consumption, flow, no_units,
                   reading_no, trans_no,
                   ALLOC_PERC, /* 2017-07 */
                   SAME_READ_ID  /* 2017-07 */ )
         SELECT NULL, custkey, order2consinv,
                CASE service
                   WHEN kwatersrv
                      THEN kinvoicewater
                   WHEN ksewersrv
                      THEN kinvoicesewer
                   WHEN kelectsrv
                      THEN kinvoiceelec
                END,
                kconsumpinvoice, charge_amt, charge_disc, prop_ref, conn_no,
                meter_type, meter_ref, c_type, tariff_id,
                CASE reading_type
                   WHEN unistr('\0041\0043\0054\0055\0041\004C')
                      THEN kreadingactual
                   WHEN unistr('\0045\0053\0054\0049\004D\005F\004F\0050')
                      THEN kreadingestimrev
                   WHEN unistr('\0045\0053\0054\0049\004D\005F\004E\004F\0050')
                      THEN kreadingestimnonrev
                   WHEN unistr('\0041\0053\0053\0045\0053\0053')
                      THEN kreadingestimnonrev
                END,
                reading, prev_reading, read_date, prev_read_date, charge_cons,
                flow, no_flats, reading_no, NULL,
                ALLOC_PERC, /* 2017-07 */
                SAME_READ_ID  /* 2017-07 */
           FROM tmp_metr_rdgs
          where not (nvl(ALLOC_PERC,0) = 0 and nvl(SAME_READ_ID,0) > 0); /* 2017-07 Exclude the actual reading that has been allocated */

      /* 2017-07 */
      ---return;

      /* ===========================================================================
         ===========================================================================
         ====  Generation of Minimum Charges  ======================================
         ===========================================================================
         =========================================================================== */

         /* ---------------------------------------------------------------------------
            Assign minimum charge and last effective date to each connection
            --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 38 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Remove all tariffs that are not currently effective - minimum charge is based
         on the tariffs that are effective as at the cycle cut-off day */
      DELETE FROM tmp_tariffs
            WHERE start_date > dtcyclecutoffday OR end_date < dtcyclecutoffday;

      /* Assign minimum charge for operational meters to each water/electricity connection */
      UPDATE tmp_cons
         SET (min_chrg_amt, min_chrg_date, min_chrg_no) =
                (SELECT t.mincharge_op, t.mincharge_date, 0
                   FROM tmp_tariffs t
                  WHERE tmp_cons.tariff_id = t.tariff_id
                    AND t.mincharge_apply = 1);

      /* Assign minimum charge for operational meters to each sewer connection */
      UPDATE tmp_s_cons
         SET (min_chrg_amt, min_chrg_date, min_chrg_no) =
                (SELECT t.mincharge_op, t.mincharge_date, 0
                   FROM tmp_tariffs t
                  WHERE tmp_s_cons.tariff_id = t.tariff_id
                    AND t.mincharge_apply = 1);

      /* Change minimum charge for non-operational meters */
      UPDATE tmp_cons
         SET min_chrg_amt = (SELECT t.mincharge_nop
                               FROM tmp_tariffs t
                              WHERE tmp_cons.tariff_id = t.tariff_id)
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_tariffs t
                 WHERE tmp_cons.tariff_id = t.tariff_id
                   AND t.mincharge_apply = 1
                   AND t.mincharge_op <> t.mincharge_nop)
         AND EXISTS (
                SELECT 1
                  FROM tmp_meters m
                 WHERE tmp_cons.meter_type = m.meter_type
                   AND tmp_cons.meter_ref = m.meter_ref
                   AND NVL (m.op_status, 0) = 1);

      /* Change sewer minimum charge for non-operational meters based on the
        operational status of the related water meter */
      UPDATE tmp_s_cons
         SET min_chrg_amt = (SELECT t.mincharge_nop
                               FROM tmp_tariffs t
                              WHERE tmp_s_cons.tariff_id = t.tariff_id)
       WHERE tmp_s_cons.water_exists = 1 /* Sewer is linked to a water connection */
         AND EXISTS (
                SELECT 1
                  FROM tmp_tariffs t
                 WHERE tmp_s_cons.tariff_id = t.tariff_id
                   AND t.mincharge_apply = 1
                   AND t.mincharge_op <> t.mincharge_nop)
         AND NOT EXISTS (
                SELECT 1
                  FROM tmp_cons c, tmp_meters m
                 WHERE c.prop_ref = tmp_s_cons.prop_ref
                   AND c.service = kwatersrv
                   AND c.meter_type =
                                    m.meter_type /* Exclude ANY connection(s)*/
                   AND c.meter_ref = m.meter_ref /* with operational meters  */
                   AND NVL (m.op_status, 0) <> 1);

      /* 2017-07 No Minumum charge for Percentage Allocated Consumption Charges */
      UPDATE tmp_cons a
         SET min_chrg_amt = null,
             min_chrg_date = null,
             min_chrg_no = null
       where exists (select 1 from tmp_metr_rdgs b
                      where a.PROP_REF = b.PROP_REF
                        and a.CONN_NO = b.CONN_NO
                        and b.SAME_READ_ID > 0);
       /* ---------------------------------------------------------------------------
         Calculate the number of minimum charges that must be charged per connection
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 39 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Compare connection date with minimum charge effective date to reduce charges for
         connections that were connected after the beginning of the current billing cycle */

      UPDATE tmp_cons
         SET min_chrg_no =
                CASE
                   WHEN conn_date > min_chrg_date
                      THEN 0
                   WHEN conn_date > min_chrg_date - TRUNC (ncyclelengthdays)
                      THEN CASE
                   WHEN TO_NUMBER (TO_CHAR (conn_date, 'DD')) >
                                     TO_NUMBER (TO_CHAR (min_chrg_date, 'DD'))
                      THEN   (  (  TO_NUMBER (TO_CHAR (min_chrg_date, 'YYYY'))
                                 - TO_NUMBER (TO_CHAR (conn_date, 'YYYY'))
                                )
                              * 12
                             )
                           + TO_NUMBER (TO_CHAR (min_chrg_date, 'MM'))
                           - TO_NUMBER (TO_CHAR (conn_date, 'MM'))
                   WHEN TO_NUMBER (TO_CHAR (conn_date, 'DD')) <=
                                     TO_NUMBER (TO_CHAR (min_chrg_date, 'DD'))
                      THEN   (  (  TO_NUMBER (TO_CHAR (min_chrg_date, 'YYYY'))
                                 - TO_NUMBER (TO_CHAR (conn_date, 'YYYY'))
                                )
                              * 12
                             )
                           + TO_NUMBER (TO_CHAR (min_chrg_date, 'MM'))
                           - TO_NUMBER (TO_CHAR (conn_date, 'MM'))
                           + 1
                END
                   ELSE ncyclelengthmonths
                END
       WHERE min_chrg_no IS NOT NULL;

      /* Do the same for sewer connections */
      UPDATE tmp_s_cons
         SET min_chrg_no =
                CASE
                   WHEN conn_date > min_chrg_date
                      THEN 0
                   WHEN conn_date > min_chrg_date - TRUNC (ncyclelengthdays)
                      THEN CASE
                   WHEN TO_NUMBER (TO_CHAR (conn_date, 'DD')) >
                                     TO_NUMBER (TO_CHAR (min_chrg_date, 'DD'))
                      THEN   (  (  TO_NUMBER (TO_CHAR (min_chrg_date, 'YYYY'))
                                 - TO_NUMBER (TO_CHAR (conn_date, 'YYYY'))
                                )
                              * 12
                             )
                           + TO_NUMBER (TO_CHAR (min_chrg_date, 'MM'))
                           - TO_NUMBER (TO_CHAR (conn_date, 'MM'))
                   WHEN TO_NUMBER (TO_CHAR (conn_date, 'DD')) <=
                                     TO_NUMBER (TO_CHAR (min_chrg_date, 'DD'))
                      THEN   (  (  TO_NUMBER (TO_CHAR (min_chrg_date, 'YYYY'))
                                 - TO_NUMBER (TO_CHAR (conn_date, 'YYYY'))
                                )
                              * 12
                             )
                           + TO_NUMBER (TO_CHAR (min_chrg_date, 'MM'))
                           - TO_NUMBER (TO_CHAR (conn_date, 'MM'))
                           + 1
                END
                   ELSE ncyclelengthmonths
                END
       WHERE min_chrg_no IS NOT NULL;


      /* Multiply by number of flats */
      UPDATE tmp_cons
         SET min_chrg_no = min_chrg_no + rev_cycles /* * (rev_cycles + 1) */
       WHERE NVL (rev_cycles, 0) > 0 AND min_chrg_no IS NOT NULL;

      /* Do the same for sewer connections */
      UPDATE tmp_s_cons
         SET min_chrg_no = min_chrg_no + rev_cycles /* * (rev_cycles + 1) */
       WHERE NVL (rev_cycles, 0) > 0 AND min_chrg_no IS NOT NULL;


      /* Clear the no of charges based on the property vacated settings */
      UPDATE tmp_cons
         SET min_chrg_no = min_chrg_no * no_flats
       WHERE no_flats > 1 AND min_chrg_no IS NOT NULL;

      /* Do the same for sewer connections */
      UPDATE tmp_s_cons
         SET min_chrg_no = min_chrg_no * no_flats
       WHERE no_flats > 1 AND min_chrg_no IS NOT NULL;

      /* Multiply by Reversed minimum charges (this is not 100% correct as we dont know
         how many months the reversed cycles applied for. Also it assumes that the number
         of charges has not been reduced due to a recent connection date which is logically
         correct since a new connection will not have reversed estimates from previous cycles */

      BEGIN
         SELECT NVL (keyword_value, 1)
           INTO nnominchargeforvacated
           FROM SETTINGS
          WHERE keyword = 'BIL_STATEMENTS_NoMinChargeForVacated';
      EXCEPTION
         WHEN OTHERS
         THEN
            nnominchargeforvacated := 1;
      END;

      IF (nnominchargeforvacated = 1)
      THEN

         /* Water/Electricity connections */
         UPDATE tmp_cons
            SET min_chrg_no = NULL
          WHERE NVL (is_vacated, 0) = 1 AND min_chrg_no IS NOT NULL;

         /* Sewer connections */
         UPDATE tmp_s_cons
            SET min_chrg_no = NULL
          WHERE NVL (is_vacated, 0) = 1 AND min_chrg_no IS NOT NULL;
      END IF;

      /* Use disconnection settings to clear the no of charges for disconnected connections  */
      BEGIN
         SELECT NVL (keyword_value, 1)
           INTO nnominchargefordiscon
           FROM SETTINGS
          WHERE keyword = 'BIL_STATEMENTS_NoMinChargeForDiscon';
      EXCEPTION
         WHEN OTHERS
         THEN
            nnominchargefordiscon := 1;
      END;

      IF (nnominchargefordiscon = 1)
      THEN

         /* Water/Electricity connections */
         UPDATE tmp_cons
            SET min_chrg_no = NULL
          WHERE NVL (con_status, 0) IN (2, 3) /* Connection disconnected */
            AND min_chrg_no IS NOT NULL;

         /* Sewer connections */
         UPDATE tmp_s_cons
            SET min_chrg_no = NULL
          WHERE min_chrg_no IS NOT NULL
            AND water_exists = 1 /* Sewer is linked to a water connection */
            AND NOT EXISTS (
                   SELECT 1
                     FROM tmp_cons c
                    WHERE c.prop_ref = tmp_s_cons.prop_ref
                      AND c.service = kwatersrv
                      AND NVL (c.con_status, 0) NOT IN
                                          (2, 3) /* Connection disconnected */);
      END IF;

      /* ---------------------------------------------------------------------------
         Calculate the minimum charge amount & discount per connection
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 40 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Calculate total amount of Minimum Charge based on the number of charges */
      UPDATE tmp_cons
         SET min_chrg_amt = min_chrg_no * min_chrg_amt
       WHERE min_chrg_no IS NOT NULL;

      /* Repeat for sewer connections */
      UPDATE tmp_s_cons
         SET min_chrg_amt = min_chrg_no * min_chrg_amt
       WHERE min_chrg_no IS NOT NULL;

      /* Apply discount */
      UPDATE tmp_cons
         SET min_chrg_disc =
                (SELECT CASE t.discount_mode
                           WHEN knodiscount
                              THEN 0
                           WHEN kdiscconsum
                              THEN 0
                           WHEN kdiscconsumandminchrg
                              THEN min_chrg_amt * t.discount_perc / 100.0
                           ELSE 0
                        END
                   FROM tmp_tariffs t
                  WHERE tmp_cons.tariff_id = t.tariff_id
                    AND t.mincharge_apply = 1
                    AND tmp_cons.min_chrg_no IS NOT NULL)
       WHERE min_chrg_no IS NOT NULL;

      /* Repeat for sewer connections */
      UPDATE tmp_s_cons
         SET min_chrg_disc =
                (SELECT CASE t.discount_mode
                           WHEN knodiscount
                              THEN 0
                           WHEN kdiscconsum
                              THEN 0
                           WHEN kdiscconsumandminchrg
                              THEN min_chrg_amt * t.discount_perc / 100.0
                           ELSE 0
                        END
                   FROM tmp_tariffs t
                  WHERE tmp_s_cons.tariff_id = t.tariff_id
                    AND t.mincharge_apply = 1
                    AND tmp_s_cons.min_chrg_no IS NOT NULL)
       WHERE min_chrg_no IS NOT NULL;

      /* Exception when discount is negative (invalid case) */
      UPDATE tmp_cons
         SET min_chrg_disc = 0
       WHERE min_chrg_disc < 0;

      /* Repeat for sewer connections */
      UPDATE tmp_s_cons
         SET min_chrg_disc = 0
       WHERE min_chrg_disc < 0;

      /* Deduct discount from amount */
      UPDATE tmp_cons
         SET min_chrg_amt = min_chrg_amt - min_chrg_disc
       WHERE min_chrg_disc > 0;

      /* Repeat for sewer connections */
      UPDATE tmp_s_cons
         SET min_chrg_amt = min_chrg_amt - min_chrg_disc
       WHERE min_chrg_disc > 0;

      /* Deduct charged amount from minimum charge */
      UPDATE tmp_cons
         SET min_chrg_amt = min_chrg_amt - NVL (charge_amt, 0),
             min_chrg_disc = min_chrg_disc - NVL (charge_disc, 0)
       WHERE min_chrg_no IS NOT NULL;

      /* Repeat for sewer connections */
      UPDATE tmp_s_cons
         SET min_chrg_amt = min_chrg_amt - NVL (charge_amt, 0),
             min_chrg_disc = min_chrg_disc - NVL (charge_disc, 0)
       WHERE min_chrg_no IS NOT NULL;

      /* Clear all zero and negative minimum charges */
      UPDATE tmp_cons
         SET min_chrg_amt = 0
       WHERE min_chrg_no IS NOT NULL AND min_chrg_amt < 0;

      UPDATE tmp_cons
         SET min_chrg_disc = 0
       WHERE min_chrg_no IS NOT NULL AND min_chrg_disc < 0;

      UPDATE tmp_cons
         SET min_chrg_amt = NULL,
             min_chrg_disc = NULL,
             min_chrg_no = NULL
       WHERE min_chrg_no IS NOT NULL
         AND min_chrg_amt <= 0.0001
         AND min_chrg_disc <= 0.0001;

      /* Repeat for sewer connections */
      UPDATE tmp_s_cons
         SET min_chrg_amt = 0
       WHERE min_chrg_no IS NOT NULL AND min_chrg_amt < 0;

      UPDATE tmp_s_cons
         SET min_chrg_disc = 0
       WHERE min_chrg_no IS NOT NULL AND min_chrg_disc < 0;

      UPDATE tmp_s_cons
         SET min_chrg_amt = NULL,
             min_chrg_disc = NULL,
             min_chrg_no = NULL
       WHERE min_chrg_no IS NOT NULL
         AND min_chrg_amt <= 0.0001
         AND min_chrg_disc <= 0.0001;

      /* ---------------------------------------------------------------------------
         Insert a transaction per minimum charge for each connection to be invoiced
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 41 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Minimum Charges for water / electricity connections */
      INSERT INTO tmp_trans
                  (trans_id, custkey, trans_order, trns_type, trns_stype,
                   amount, discount, prop_ref, conn_no, meter_type, meter_ref,
                   c_type, tariff_id, read_type, reading, prev_reading,
                   read_date, prev_read_date, consumption, no_units,
                   reading_no, trans_no)
         SELECT NULL, custkey, order3mincharge,
                CASE service
                   WHEN kwatersrv
                      THEN kinvoicewater
                   WHEN kelectsrv
                      THEN kinvoiceelec
                END,
                kminchargesadj, min_chrg_amt, min_chrg_disc, prop_ref,
                conn_no, meter_type, meter_ref, c_type, tariff_id, NULL, NULL,
                NULL, NULL, NULL, NULL, min_chrg_no, NULL, NULL
           FROM tmp_cons
          WHERE min_chrg_no IS NOT NULL;

      /* Minimum Charges for sewer connections */
      INSERT INTO tmp_trans
                  (trans_id, custkey, trans_order, trns_type, trns_stype,
                   amount, discount, prop_ref, conn_no, meter_type, meter_ref,
                   c_type, tariff_id, read_type, reading, prev_reading,
                   read_date, prev_read_date, consumption, no_units,
                   reading_no, trans_no)
         SELECT NULL, custkey, order3mincharge, kinvoicesewer, kminchargesadj,
                min_chrg_amt, min_chrg_disc, prop_ref, conn_no, NULL, NULL,
                c_type, tariff_id, NULL, NULL, NULL, NULL, NULL, NULL,
                min_chrg_no, NULL, NULL
           FROM tmp_s_cons
          WHERE min_chrg_no IS NOT NULL;

      /* ===========================================================================
         ===========================================================================
         ====  Generation of Regular charges  ======================================
         ===========================================================================
         =========================================================================== */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 42 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      kregchargemin := 50;
      kregchargemax := 70;

      /* Find the most recent cutoff date for a cycle for the same billing group and increment by one */
      BEGIN
         SELECT   TRUNC (NVL (MAX (bilng_date),
                              (dtcyclecutoffday - ncyclelengthdays
                              )
                             )
                        )
                + 1
           INTO dtcyclestartdate
           FROM sum_bcyc
          WHERE billgroup = strbillgroup AND cycle_clsd = 1;
      EXCEPTION
         WHEN OTHERS
         THEN
            dtcyclestartdate :=
                               TRUNC (dtcyclecutoffday - ncyclelengthdays)
                               + 1;
      END;

      /* Declare cursor to process applicable charges sequentially */
      FOR cur_charge IN
         (SELECT trns_code AS trns_type, trns_scode AS trns_stype,
                 NVL (chrg_perbill, 0) AS chrg_perbill,
                 NVL (chrg_effective_date,
                      TO_DATE ('01-01-80', 'DD-MM-YY')
                     ) AS chrg_effective_date,
                 CASE
                    WHEN chrg_interval IS NULL
                       THEN 1
                    WHEN chrg_interval <= 0
                       THEN 1
                    ELSE chrg_interval
                 END AS chrg_interval,
                 CASE
                    WHEN chrg_monthday IS NULL
                       THEN 1
                    WHEN chrg_monthday <= 0
                       THEN 1
                    ELSE chrg_monthday
                 END AS chrg_monthday
            FROM transaction_types
           WHERE trns_code IN
                    (kinvoicewater,
                     kinvoicesewer,
                     kinvoiceelec,
                     kinvoicerefuse,
                     kinvoiceborehole,
                     kinvoiceratestax,
                     kinvoiceuser1,
                     kinvoiceuser2
                    )
             AND trns_scode >= kregchargemin
             AND trns_scode <= kregchargemax
             AND NVL (is_chargeable, 0) = 1)
      LOOP /* Decode Date */
         nchrgyear :=
                 TO_NUMBER (TO_CHAR (cur_charge.chrg_effective_date, 'YYYY'));
         nchrgmonth :=
                   TO_NUMBER (TO_CHAR (cur_charge.chrg_effective_date, 'MM'));

         WHILE (0 = 0)
         LOOP /* Increment charge month by interval and adjust year if necessary */
            nchrgmonth := nchrgmonth + cur_charge.chrg_interval;

            WHILE nchrgmonth > 12
            LOOP
               nchrgmonth := nchrgmonth - 12;
               nchrgyear := nchrgyear + 1;
            END LOOP; /* Define the charge day */

            nchrgday := cur_charge.chrg_monthday;

            IF (nchrgday = 31) AND (nchrgmonth IN (4, 6, 9, 11))
            THEN
               nchrgday := 30;
            END IF;

            IF (nchrgday > 28) AND (nchrgmonth = 2)
            THEN
               nchrgday := 28;
            END IF; /* Define the charge date */

            nchrgdate :=
               TO_DATE (   LPAD (nchrgday, 2, '0')
                        || '-'
                        || LPAD (nchrgmonth, 2, '0')
                        || '-'
                        || LPAD (nchrgyear, 4, '0'),
                        'DD-MM-YYYY'
                       );

            /* For "Charge per Billing cycle" cases force the cutoff date as charge date */
            IF cur_charge.chrg_perbill = 1
            THEN
               nchrgdate := dtcyclecutoffday;
            END IF;

            /* Insert in table if the charge date is within the current cycle date range */
            IF nchrgdate >= dtcyclestartdate AND nchrgdate <= dtcyclecutoffday
            THEN
               INSERT INTO tmp_regcharge_dates
                           (trns_type, trns_stype, service, charge_date,
                            charge_type, charge_disconn, charge_vacated,
                            charge_owner, fixed_charge, lookup_relation,
                            entity_relation)
                  SELECT trns_code, trns_scode,
                         CASE trns_code
                            WHEN kinvoicewater
                               THEN kwatersrv
                            WHEN kinvoicesewer
                               THEN ksewersrv
                            WHEN kinvoiceelec
                               THEN kelectsrv
                            WHEN kinvoicerefuse
                               THEN krefsrv
                            WHEN kinvoiceborehole
                               THEN kboreholesrv
                            WHEN kinvoiceratestax
                               THEN kratestaxsrv
                            WHEN kinvoiceuser1
                               THEN kuser1
                            WHEN kinvoiceuser2
                               THEN kuser2
                         END,
                         nchrgdate, NVL (chrgtype, 0),
                         NVL (fixedcharge_disc, 0),
                         NVL (fixedcharge_vacated, 0),
                         NVL (charge_prop_owner, 0), NVL (fixed_charge, 0),
                         NVL (charge_lu_rel, -1),
                         NVL (chrg_entity_relation, 0)
                    FROM transaction_types t
                   WHERE t.trns_code = cur_charge.trns_type
                     AND t.trns_scode = cur_charge.trns_stype;
            END IF;

            /* Stop when charge date is greater or equal to the billing cutoff date */
            IF nchrgdate >= dtcyclecutoffday
            THEN
               EXIT;
            END IF;
         END LOOP;
      END LOOP;


      /* ---------------------------------------------------------------------------
         Insert all regular charges per date into a new table per entity
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 43 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Declare Regular Charge constants */
      kmtrentity := 0;
      kconnentity := 1;
      kpropentity := 2;
      knorelation := -1;
      kuserdefined := 0;
      kvariablepertariff := 1;
      kvariablepermetersize := 2;
      kvariableperconsumertype := 3;
      kvariableperconsumpttype := 4;
      kvariableperinfoflag1 := 5;
      kvariableperinfoflag2 := 6;
      kvariableperinfoflag3 := 7;
      kvariableperinfoflag4 := 8;
      kvariableperinfoflag5 := 9;

      /* Insert regular charges related to meters */
      INSERT INTO tmp_reg_charges
                  (custkey, trns_type, trns_stype, charge_date, prop_ref,
                   conn_no, meter_type, meter_ref, c_type, tariff_id, amount,
                   lookup_code)
         SELECT m.custkey, r.trns_type, r.trns_stype, r.charge_date,
                m.prop_ref, m.conn_no, m.meter_type, m.meter_ref, m.c_type,
                m.tariff_id, 1,
                CASE r.lookup_relation
                   WHEN kvariablepertariff
                      THEN m.tariff_id
                   WHEN kvariablepermetersize
                      THEN m.mtr_size
                   WHEN kvariableperconsumpttype
                      THEN m.c_type
                   WHEN kvariableperinfoflag1
                      THEN m.info_flag1
                   WHEN kvariableperinfoflag2
                      THEN m.info_flag2
                   WHEN kvariableperinfoflag3
                      THEN m.info_flag3
                   WHEN kvariableperinfoflag4
                      THEN m.info_flag4
                   WHEN kvariableperinfoflag5
                      THEN m.info_flag5
                   ELSE NULL
                END
           FROM tmp_meters m, tmp_regcharge_dates r
          WHERE r.entity_relation = kmtrentity
            AND r.service = m.service
            AND r.charge_date > m.conn_date
            AND (   (    m.is_vacated = 1
                     AND r.lookup_relation = kvariablepertariff
                    )
                 OR r.charge_vacated = 0
                ) /* Charge regardless of property vacated status */
            AND (m.con_status NOT IN (2, 3) OR /* Connection is not disconnected */ r.charge_disconn <>
                                                                                      0
                ); /* Charge disconnected connections */

      /* Insert regular charges related to water/electricity connections */
      INSERT INTO tmp_reg_charges
                  (custkey, trns_type, trns_stype, charge_date, prop_ref,
                   conn_no, meter_type, meter_ref, c_type, tariff_id, amount,
                   lookup_code)
         SELECT c.custkey, r.trns_type, r.trns_stype, r.charge_date,
                c.prop_ref, c.conn_no, c.meter_type, c.meter_ref, c.c_type,
                c.tariff_id, 1,
                CASE r.lookup_relation
                   WHEN kvariablepertariff
                      THEN c.tariff_id
                   WHEN kvariableperconsumpttype
                      THEN c.c_type
                   WHEN kvariableperinfoflag1
                      THEN c.info_flag1
                   WHEN kvariableperinfoflag2
                      THEN c.info_flag2
                   WHEN kvariableperinfoflag3
                      THEN c.info_flag3
                   WHEN kvariableperinfoflag4
                      THEN c.info_flag4
                   WHEN kvariableperinfoflag5
                      THEN c.info_flag5
                   ELSE NULL
                END
           FROM tmp_cons c, tmp_regcharge_dates r
          WHERE r.entity_relation = kconnentity
            AND r.service = c.service
            AND r.charge_date > c.conn_date
            AND (   (    c.is_vacated = 1
                     AND r.lookup_relation = kvariablepertariff
                    )
                 OR r.charge_vacated = 0
                ) /* Charge regardless of property vacated status */
            AND (c.con_status NOT IN (2, 3) OR /* Connection is not disconnected */ r.charge_disconn <>
                                                                                      0
                ) /* Charge disconnected connections */
            AND r.lookup_relation <> kvariablepermetersize; /* Invalid combination for connections */

      /* Insert regular charges related to sewer connections */
      INSERT INTO tmp_reg_charges
                  (custkey, trns_type, trns_stype, charge_date, prop_ref,
                   conn_no, meter_type, meter_ref, c_type, tariff_id, amount,
                   lookup_code)
         SELECT s.custkey, r.trns_type, r.trns_stype, r.charge_date,
                s.prop_ref, s.conn_no, NULL, NULL, s.c_type, s.tariff_id, 1,
                CASE r.lookup_relation
                   WHEN kvariablepertariff
                      THEN s.tariff_id
                   WHEN kvariableperconsumpttype
                      THEN s.c_type
                   WHEN kvariableperinfoflag1
                      THEN s.info_flag1
                   WHEN kvariableperinfoflag2
                      THEN s.info_flag2
                   WHEN kvariableperinfoflag3
                      THEN s.info_flag3
                   WHEN kvariableperinfoflag4
                      THEN s.info_flag4
                   WHEN kvariableperinfoflag5
                      THEN s.info_flag5
                   ELSE NULL
                END
           FROM tmp_s_cons s, tmp_regcharge_dates r
          WHERE r.entity_relation = kconnentity
            AND r.service = ksewersrv
            AND r.charge_date > s.conn_date
            AND (   (    s.is_vacated = 1
                     AND r.lookup_relation = kvariablepertariff
                    )
                 OR r.charge_vacated = 0
                ) /* Charge regardless of property vacated status */
            AND r.lookup_relation <> kvariablepermetersize; /* Invalid combination for connections */

      /* Insert regular charges related to Properties */
      INSERT INTO tmp_reg_charges
                  (custkey, trns_type, trns_stype, charge_date, prop_ref,
                   conn_no, meter_type, meter_ref, c_type, tariff_id, amount,
                   lookup_code)
         SELECT p.custkey, r.trns_type, r.trns_stype, r.charge_date,
                p.prop_ref, NULL, NULL, NULL, p.c_type, NULL, 1,
                CASE r.lookup_relation
                   WHEN kvariableperconsumpttype
                      THEN p.c_type
                   WHEN kvariableperinfoflag1
                      THEN p.info_flag1
                   WHEN kvariableperinfoflag2
                      THEN p.info_flag2
                   WHEN kvariableperinfoflag3
                      THEN p.info_flag3
                   WHEN kvariableperinfoflag4
                      THEN p.info_flag4
                   WHEN kvariableperinfoflag5
                      THEN p.info_flag5
                   ELSE NULL
                END
           FROM tmp_props p, tmp_regcharge_dates r
          WHERE r.entity_relation = kpropentity
            AND (   (r.service = kwatersrv AND p.srv1_alcto = 1)
                 OR (r.service = ksewersrv AND p.srv2_alcto = 1)
                 OR (r.service = kelectsrv AND p.srv3_alcto = 1)
                 OR (r.service = krefsrv AND p.srv4_alcto = 1)
                 OR (r.service = kboreholesrv AND p.srv5_alcto = 1)
                 OR (r.service = kratestaxsrv AND p.srv6_alcto = 1)
                 OR (r.service = kuser1 AND p.srv7_alcto = 1)
                 OR (r.service = kuser2 AND p.srv8_alcto = 1)
                )
            AND r.lookup_relation <>
                    kvariablepertariff /* Invalid combination for properties */
            AND r.lookup_relation <>
                   kvariablepermetersize /* Invalid combination for properties */
            AND (r.charge_owner <> 1 OR r.service <> kratestaxsrv); /* Exclude rates for prop owners */

      /* Insert regular charges related to Property Owners */
      INSERT INTO tmp_reg_charges
                  (custkey, trns_type, trns_stype, charge_date, prop_ref,
                   conn_no, meter_type, meter_ref, c_type, tariff_id, amount,
                   lookup_code)
         SELECT p.custkey, r.trns_type, r.trns_stype, r.charge_date,
                p.prop_ref, NULL, NULL, NULL, p.c_type, NULL,
                CASE
                   WHEN p.share_denominator = 0
                      THEN 0
                   ELSE   p.prop_value
                        * p.share_numerator
                        / p.share_denominator
                        / 1000.0
                END,
                CASE r.lookup_relation
                   WHEN kvariableperconsumpttype
                      THEN p.c_type
                   WHEN kvariableperinfoflag1
                      THEN p.info_flag1
                   WHEN kvariableperinfoflag2
                      THEN p.info_flag2
                   WHEN kvariableperinfoflag3
                      THEN p.info_flag3
                   WHEN kvariableperinfoflag4
                      THEN p.info_flag4
                   WHEN kvariableperinfoflag5
                      THEN p.info_flag5
                   ELSE NULL
                END
           FROM tmp_prop_owners p, tmp_regcharge_dates r
          WHERE r.entity_relation = kpropentity
            AND r.lookup_relation <>
                    kvariablepertariff /* Invalid combination for properties */
            AND r.lookup_relation <>
                   kvariablepermetersize /* Invalid combination for properties */
            AND r.charge_owner = 1
            AND r.service = kratestaxsrv;

      /* ---------------------------------------------------------------------------
         Calculate amounts for each charge date, for each regular charge
         --------------------------------------------------------------------------- */

      kfixedcharge := 0;

      /* Process all fixed charges */
      UPDATE tmp_reg_charges
         SET amount =
                  amount
                * (SELECT r.fixed_charge
                     FROM tmp_regcharge_dates r
                    WHERE r.trns_type = tmp_reg_charges.trns_type
                      AND r.trns_stype = tmp_reg_charges.trns_stype
                      AND r.charge_date = tmp_reg_charges.charge_date
                      AND r.charge_type = kfixedcharge)
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_regcharge_dates r
                 WHERE r.trns_type = tmp_reg_charges.trns_type
                   AND r.trns_stype = tmp_reg_charges.trns_stype
                   AND r.charge_date = tmp_reg_charges.charge_date
                   AND r.charge_type = kfixedcharge);

      /* Update all charges that are variable per meter size */
      UPDATE tmp_reg_charges
         SET amount =
                  amount
                * (SELECT CASE
                             WHEN NVL (l.flag, 0) > 0
                             AND NVL (l.flag, 0) <= c.charge_cons
                                THEN l.perunit
                             ELSE l.FIXED
                          END
                     FROM tmp_regcharge_dates r, tmp_cons c,
                          charge_lu_table l
                    WHERE tmp_reg_charges.trns_type = r.trns_type
                      AND tmp_reg_charges.trns_stype = r.trns_stype
                      AND tmp_reg_charges.charge_date = r.charge_date
                      AND tmp_reg_charges.prop_ref = c.prop_ref
                      AND tmp_reg_charges.conn_no = c.conn_no
                      AND r.service = c.service
                      AND r.trns_type = l.trns_code
                      AND r.trns_stype = l.trns_scode
                      AND tmp_reg_charges.lookup_code = l.code
                      AND r.lookup_relation = kvariablepermetersize)
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_regcharge_dates r, tmp_cons c, charge_lu_table l
                 WHERE tmp_reg_charges.trns_type = r.trns_type
                   AND tmp_reg_charges.trns_stype = r.trns_stype
                   AND tmp_reg_charges.charge_date = r.charge_date
                   AND tmp_reg_charges.prop_ref = c.prop_ref
                   AND tmp_reg_charges.conn_no = c.conn_no
                   AND r.service = c.service
                   AND r.trns_type = l.trns_code
                   AND r.trns_stype = l.trns_scode
                   AND tmp_reg_charges.lookup_code = l.code
                   AND r.lookup_relation = kvariablepermetersize);

      /* Update all charges that are variable per lookup other than meter size */
      UPDATE tmp_reg_charges
         SET amount =
                  amount
                * (SELECT l."FIXED"
                     FROM tmp_regcharge_dates r, charge_lu_table l
                    WHERE tmp_reg_charges.trns_type = r.trns_type
                      AND tmp_reg_charges.trns_stype = r.trns_stype
                      AND tmp_reg_charges.charge_date = r.charge_date
                      AND r.trns_type = l.trns_code
                      AND r.trns_stype = l.trns_scode
                      AND tmp_reg_charges.lookup_code = l.code
                      AND r.lookup_relation IN
                             (kvariablepertariff,
                              kvariableperconsumpttype,
                              kvariableperinfoflag1,
                              kvariableperinfoflag2,
                              kvariableperinfoflag3,
                              kvariableperinfoflag4,
                              kvariableperinfoflag5
                             ))
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_regcharge_dates r, charge_lu_table l
                 WHERE tmp_reg_charges.trns_type = r.trns_type
                   AND tmp_reg_charges.trns_stype = r.trns_stype
                   AND tmp_reg_charges.charge_date = r.charge_date
                   AND r.trns_type = l.trns_code
                   AND r.trns_stype = l.trns_scode
                   AND tmp_reg_charges.lookup_code = l.code
                   AND r.lookup_relation IN
                          (kvariablepertariff,
                           kvariableperconsumpttype,
                           kvariableperinfoflag1,
                           kvariableperinfoflag2,
                           kvariableperinfoflag3,
                           kvariableperinfoflag4,
                           kvariableperinfoflag5
                          ));

      /* Adjust Refuse charges according to the number of bins */
      UPDATE tmp_reg_charges
         SET amount =
                (SELECT   amount
                        * CASE
                             WHEN NVL (p.rfs_nobins, 0) < 1
                                THEN 1
                             ELSE p.rfs_nobins
                          END
                   FROM tmp_props p
                  WHERE tmp_reg_charges.trns_type = kinvoicerefuse
                    AND tmp_reg_charges.prop_ref = p.prop_ref
                    AND p.srv3_alcto = 1)
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_props p
                 WHERE trns_type = kinvoicerefuse
                   AND prop_ref = p.prop_ref
                   AND p.srv3_alcto = 1);

      /* ---------------------------------------------------------------------------
         Insert a transaction per regular charge to be invoiced
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 45 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      INSERT INTO tmp_trans
                  (trans_id, custkey, trans_order, trns_type, trns_stype,
                   amount, discount, prop_ref, conn_no, meter_type, meter_ref,
                   c_type, tariff_id, read_type, reading, prev_reading,
                   read_date, prev_read_date, consumption, flow, no_units,
                   reading_no, trans_no)
         SELECT NULL, custkey, order4regcharge, trns_type, trns_stype, amount,
                0, prop_ref, conn_no, meter_type, meter_ref, c_type,
                tariff_id, NULL, NULL, NULL, charge_date, NULL, /* Cheat and put in available date field */ NULL,
                NULL, NULL, NULL, NULL
           FROM tmp_reg_charges
          WHERE amount IS NOT NULL AND amount > 0;

      /* Special case for rates (LSDB) for properties with total charge less than 1 */
      /*          *** To be redesigned and revised in due course ***                */
      INSERT INTO tmp_trans
                  (custkey, trans_order, trns_type, trns_stype, amount,
                   discount, prop_ref, conn_no, meter_type, meter_ref, c_type,
                   tariff_id, read_type, reading, prev_reading, read_date,
                   prev_read_date, consumption, flow, no_units, reading_no,
                   trans_no)
         SELECT c.custkey, order4regcharge, c.trns_type, c.trns_stype,
                0 - c.amount, 0, c.prop_ref, c.conn_no, /* Reverse Charge */ c.meter_type,
                c.meter_ref, c.c_type, c.tariff_id, NULL, NULL, NULL,
                c.charge_date, NULL, /* Cheat and put in available date field */ NULL,
                NULL, NULL, NULL, NULL
           FROM tmp_reg_charges c,
                (SELECT   custkey, SUM (amount) AS tot_amount
                     FROM tmp_reg_charges c
                    WHERE amount IS NOT NULL AND trns_type = kinvoiceratestax
                 GROUP BY custkey
                   HAVING SUM (amount) < 1) m
          WHERE c.amount IS NOT NULL
            AND c.amount <= 1
            AND c.trns_type = kinvoiceratestax
            AND c.custkey = m.custkey;

      /* ===========================================================================
         ===========================================================================
         ====  Generation of Surharges  ============================================
         ===========================================================================
         =========================================================================== */

         /*  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
         |                                                 |
         |   Postponed - To be implemented for Lusaka      |
         |                                                 |
         ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  */

      /* ===========================================================================
         ===========================================================================
         ====  Actual Database Updates (Point of no return )  ======================
         ===========================================================================
         =========================================================================== */

         /* ---------------------------------------------------------------------------
         Reverse old estimates
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 46 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Mark identified estimates in F_TRANS as reversed */
      UPDATE f_trans
         SET rev_cycle_id = p_nbillcycleid
       WHERE billgroup = strbillgroup
         AND EXISTS (
                SELECT 1
                  FROM tmp_rev_estim r
                 WHERE f_trans.custkey = r.custkey
                   AND f_trans.trans_no = r.trans_no);

      /* ---------------------------------------------------------------------------
         Update transactions with Ledger Accounts
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 47 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      UPDATE tmp_trans
         SET (serv_catg, item_code, sitem_code) =
                (SELECT t.gl_srvctg, t.gl_itmcod, t.gl_sitmcod
                   FROM transaction_types t
                  WHERE tmp_trans.trns_type = t.trns_code
                    AND tmp_trans.trns_stype = t.trns_scode);

      /* ---------------------------------------------------------------------------
         Safeguards for the transaction table
         --------------------------------------------------------------------------- */

      /* Zero NULL amounts - normally none here */
      UPDATE tmp_trans
         SET amount = 0
       WHERE amount IS NULL;

      /* Zero NULL discount - possibly from old data */
      UPDATE tmp_trans
         SET discount = 0
       WHERE discount IS NULL;

      /* ---------------------------------------------------------------------------
         Insert a transaction in F_TRANS for each record in TMP_TRANS
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 48 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF; /* Assign a transaction ID per record */

      UPDATE tmp_trans
         SET trans_id = ROWNUM;

      /* Declare cursor to process transactions sequentially */

      ntotrecords := SQL%ROWCOUNT;
      progress.initialise_record (p_nprocessid, ntotrecords);
      COMMIT;

      /* Initialise counters and variables */

      SELECT COUNT (1)
        INTO ntotrecords
        FROM tmp_trans;

      ncount := 0;
      nincrement := ntotrecords / 50;

      IF nincrement < 1
      THEN
         nincrement := 1;
      END IF;

      strcurcustkey := '####';

      /* Loop processing records */
      FOR cur_tmptrans IN (SELECT   trans_id, custkey, trans_order, trns_type,
                                    trns_stype, amount, discount, prop_ref,
                                    conn_no, meter_type, meter_ref, c_type,
                                    tariff_id, read_type, reading,
                                    prev_reading, read_date, prev_read_date,
                                    consumption, no_units, serv_catg,
                                    item_code, sitem_code, READING_NO /* 2017-07 */
                               FROM tmp_trans
                           ORDER BY custkey,
                                    prop_ref,
                                    trans_order,
                                    trns_type,
                                    trns_stype)
      LOOP /* Update progress monitor */
         IF ncount = nincrement
         THEN
            progress.start_new_record (p_nprocessid,
                                       cur_tmptrans.custkey,
                                       nincrement
                                      );
            COMMIT;
            ncount := 0;
         END IF;

         ncount := ncount + 1;
        /* Identify next transaction number for current customer */

         --------------------------MODIFIY EGYPT 2020
         ---IF strcurcustkey <> cur_tmptrans.custkey
         ---THEN
            BEGIN
               SELECT NVL (MIN (trans_no), 0)
                 INTO ntmptransno
                 FROM vwf_trans
                WHERE custkey = cur_tmptrans.custkey;
            EXCEPTION
               WHEN OTHERS
               THEN
                  ntmptransno := 0;
            END;

            strcurcustkey := cur_tmptrans.custkey;
         ---END IF;

         ntmptransno := ntmptransno - 1;

         /* Round Consumption to 2 decimal places */
         IF cur_tmptrans.consumption IS NOT NULL
         THEN
            billing.roundto (1 /* Round to */,
                             2 /* Decimals */,
                             1 /* Round Nearest */,
                             cur_tmptrans.consumption,
                             nroundingdiff
                            );
         END IF;

         /* Round amount and discount (if any) */
         ntotalamount := cur_tmptrans.amount + cur_tmptrans.discount;
         ntmpdiscount := cur_tmptrans.discount;

         IF nissueroundingdiff = 1
         THEN
            billing.roundto (1,
                             nnoofdigits,
                             nroundingmethod,
                             ntotalamount,
                             nroundingdiff
                            );
            billing.roundto (1,
                             nnoofdigits,
                             nroundingmethod,
                             cur_tmptrans.amount,
                             nroundingdiff
                            );
            ntmpdiscount := ntotalamount - cur_tmptrans.amount;

            IF ntmpdiscount < 0.0
            THEN
               ntmpdiscount := 0;
            END IF;
         ELSE
            billing.roundto (nroundto,
                             nnoofdigits,
                             1 /* Round Nearest */,
                             ntotalamount,
                             nroundingdiff
                            );
            billing.roundto (nroundto,
                             nnoofdigits,
                             1 /* Round Nearest */,
                             cur_tmptrans.amount,
                             nroundingdiff
                            );
            ntmpdiscount := ntotalamount - cur_tmptrans.amount;

            IF ntmpdiscount < 0.0
            THEN
               ntmpdiscount := 0;
            END IF;
         END IF;

         /* Skip non consumption transactions where amount and discount are zero */
         IF (   cur_tmptrans.amount <> 0.0
             OR ntmpdiscount = 0.0
             OR cur_tmptrans.trans_order = order2consinv
            )
         THEN

            /* Set effective date for regular charges */
            IF cur_tmptrans.trans_order = order4regcharge
            THEN
               dteffectdate := cur_tmptrans.read_date;
               dttmpdateto := NULL;
            ELSE
               dteffectdate := dtcyclecutoffday;
               dttmpdateto := cur_tmptrans.read_date;
            END IF;

            /* Insert new transaction in F_TRANS */
            INSERT INTO f_trans
                        (billgroup, custkey, trans_no, statm_no,
                         effect_dte, cancelled, reversed, trns_type,
                         trns_stype, serv_catg,
                         item_code, sitem_code,
                         prop_ref, conn_no,
                         meter_type, meter_ref,
                         c_type, read_type,
                         tariff_id, date_from,
                         date_to, pr_reading,
                         cr_reading, consump,
                         amount, discount, allocated_amount,
                         no_units, stamp_date, stamp_user,
                         bill_cycle_id, bill_cycle_step, rev_cycle_id,
                         READING_NO /* 2017-07 */
                        )
                 VALUES (strbillgroup, cur_tmptrans.custkey, ntmptransno, 0,
                         dteffectdate, 0, 0, cur_tmptrans.trns_type,
                         cur_tmptrans.trns_stype, cur_tmptrans.serv_catg,
                         cur_tmptrans.item_code, cur_tmptrans.sitem_code,
                         cur_tmptrans.prop_ref, cur_tmptrans.conn_no,
                         cur_tmptrans.meter_type, cur_tmptrans.meter_ref,
                         cur_tmptrans.c_type, cur_tmptrans.read_type,
                         cur_tmptrans.tariff_id, cur_tmptrans.prev_read_date,
                         dttmpdateto, cur_tmptrans.prev_reading,
                         cur_tmptrans.reading, cur_tmptrans.consumption,
                         cur_tmptrans.amount, ntmpdiscount, 0,
                         cur_tmptrans.no_units, SYSDATE, p_strcurruser,
                         p_nbillcycleid, kthisstep, kundefinedcycle,
                         cur_tmptrans.READING_NO /* 2017-07 */
                        );

            /* Save TRANS_NO in TMP_TRANS so that METR_RDG can be updated later on */
            UPDATE tmp_trans
               SET trans_no = ntmptransno,
                   consumption = cur_tmptrans.consumption /* Rounded */
             WHERE trans_id = cur_tmptrans.trans_id;
         END IF;
      END LOOP;

      /* ---------------------------------------------------------------------------
         Update METR_RDG with invoicing information for water connections
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 49 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Update water readings with charge information */
      UPDATE metr_rdg /* 2017-07 */
         SET (is_invoicd, date_invcd, consump, flow, prop_ref, conn_no,
              billgroup, custkey, wtrans_no, upd_cycle_id) =
                (SELECT 1, dtcyclecutoffday, sum(t.consumption), sum(t.flow),
                        t.prop_ref, t.conn_no, strbillgroup, t.custkey,
                        min(t.trans_no), p_nbillcycleid
                   FROM tmp_trans t
                  WHERE t.reading_no IS NOT NULL
                    AND t.read_type = kreadingactual
                    AND t.trns_type = kinvoicewater
                    AND metr_rdg.meter_type = t.meter_type
                    AND metr_rdg.meter_ref = t.meter_ref
                    AND metr_rdg.reading_no = t.reading_no
                 GROUP BY dtcyclecutoffday, t.prop_ref, t.conn_no,
                          strbillgroup, t.custkey, p_nbillcycleid
                 )
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_trans t2
                 WHERE metr_rdg.meter_type = t2.meter_type
                   AND metr_rdg.meter_ref = t2.meter_ref
                   AND metr_rdg.reading_no = t2.reading_no
                   AND t2.reading_no IS NOT NULL
                   AND t2.read_type = kreadingactual
                   AND t2.trns_type = kinvoicewater);


      /* ---------------------------------------------------------------------------
         Update METR_RDG with invoicing information for electricity connections
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 50 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

      /* Update electricity readings with charge information */
      UPDATE metr_rdg
         SET (is_invoicd, date_invcd, consump, flow, prop_ref, conn_no,
              billgroup, custkey, etrans_no, upd_cycle_id) =
                (SELECT 1, dtcyclecutoffday, t.consumption, t.flow,
                        t.prop_ref, t.conn_no, strbillgroup, t.custkey,
                        t.trans_no, p_nbillcycleid
                   FROM tmp_trans t
                  WHERE t.reading_no IS NOT NULL
                    AND t.read_type = kreadingactual
                    AND t.trns_type = kinvoiceelec
                    AND metr_rdg.meter_type = t.meter_type
                    AND metr_rdg.meter_ref = t.meter_ref
                    AND metr_rdg.reading_no = t.reading_no)
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_trans t2
                 WHERE t2.reading_no IS NOT NULL
                   AND t2.read_type = kreadingactual
                   AND t2.trns_type = kinvoiceelec
                   AND metr_rdg.meter_type = t2.meter_type
                   AND metr_rdg.meter_ref = t2.meter_ref
                   AND metr_rdg.reading_no = t2.reading_no);

      /* ---------------------------------------------------------------------------
         Update METR_RDG with invoicing information for sewer connections
         --------------------------------------------------------------------------- */

      progress.start_new_phase (p_nprocessid, 0, nretval); /* Step 51 */
      COMMIT;

      IF (nretval != 1)
      THEN
         RETURN;
      END IF;

       /* Update water readings with related sewer charge information */

     /* UPDATE metr_rdg
         SET strans_no =
                (SELECT t.trans_no
                   FROM tmp_trans t
                  WHERE t.reading_no IS NOT NULL
                    AND t.read_type = kreadingactual
                    AND t.trns_type = kinvoicesewer
                    AND metr_rdg.meter_type = t.meter_type
                    AND metr_rdg.meter_ref = t.meter_ref
                    AND metr_rdg.reading_no = t.reading_no
                    )
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_trans t2
                 WHERE t2.reading_no IS NOT NULL
                   AND t2.read_type = kreadingactual
                   AND t2.trns_type = kinvoicesewer
                   AND metr_rdg.meter_type = t2.meter_type
                   AND metr_rdg.meter_ref = t2.meter_ref
                   AND metr_rdg.reading_no = t2.reading_no);*/

      --------------------------EGYPT 2020

      UPDATE metr_rdg
         SET strans_no =
                (SELECT min(t.trans_no)
                   FROM tmp_trans t
                  WHERE t.reading_no IS NOT NULL
                    AND t.read_type = kreadingactual
                    AND t.trns_type = kinvoicesewer
                    AND metr_rdg.meter_type = t.meter_type
                    AND metr_rdg.meter_ref = t.meter_ref
                    AND metr_rdg.reading_no = t.reading_no
                    GROUP BY dtcyclecutoffday, t.prop_ref, t.conn_no,
                          strbillgroup, t.custkey, p_nbillcycleid )
       WHERE EXISTS (
                SELECT 1
                  FROM tmp_trans t2
                 WHERE t2.reading_no IS NOT NULL
                   AND t2.read_type = kreadingactual
                   AND t2.trns_type = kinvoicesewer
                   AND metr_rdg.meter_type = t2.meter_type
                   AND metr_rdg.meter_ref = t2.meter_ref
                   AND metr_rdg.reading_no = t2.reading_no);


     /* ---------------------------------------------------------------------------
        End of SP
        --------------------------------------------------------------------------- */

      progress.end_of_process (p_nprocessid);
      COMMIT;
   END invoicesforconsumption;


   PROCEDURE generatestmtuser  (p_strbillgroup   NVARCHAR2,
                               p_dtbillcycledate DATE,
                               p_nbillcycleid    NUMBER,
                               p_nstation        NUMBER,
                               p_strcurrentuser  NVARCHAR2,
                               p_nprocessid      NUMBER)
   as
     nCount number(10);
   BEGIN
     nCount := 0;
     /* Put your custom update for F_STATM here */
   end;

  PROCEDURE ReverseIndividual (p_strCustkey      NVARCHAR2,
                               p_nbillcycleid    NUMBER)
  as
    strBillgroup nvarchar2(10);
    dBilngDate date;
  begin

     select BILLGROUP, BILNG_DATE into strBillgroup, dBilngDate
       from SUM_BCYC
      where CYCLE_ID = p_nbillcycleid;

     /* Reverse Step 6 */
     delete from F_STATM
      where CUSTKEY = p_strcustkey
        and BILLGROUP  = strbillgroup
        and BILNG_DATE = dbilngdate;

     update F_TRANS
        set STATM_NO = 0,
            BILNG_DATE = null
      where CUSTKEY = p_strcustkey
        and BILLGROUP  = strbillgroup
        and BILNG_DATE = dbilngdate ;

     delete from F_TRANS
      where CUSTKEY = p_strcustkey
        and BILL_CYCLE_ID = p_nbillcycleid
        and BILL_CYCLE_STEP = 6;

     /* Reverse Step 4 */
     update CUST_AGREEM
        set AGRM_STATUS = 0
      where AGRM_STATUS = 1
        and CUSTKEY = p_strcustkey
        and exists (select 1 from AGREEMENT_INSTALLMENTS ai
                     where CUST_AGREEM.CUSTKEY = ai.CUSTKEY
                       and CUST_AGREEM.AGRM_NO = ai.AGRM_NO
                       and exists (select 1 from F_TRANS ft
                                    where ai.CUSTKEY = ft.CUSTKEY
                                      and ai.TRANS_NO = ft.TRANS_NO
                                      and ft.BILL_CYCLE_ID =  p_nbillcycleid
                                      and ft.BILL_CYCLE_STEP =  4 ));

     update AGREEMENT_INSTALLMENTS
        set CHARGE_DATE = to_date(sysdate,'DD-MON-YYYY')
      where CUSTKEY = p_strcustkey
        and exists (select 1 from F_TRANS ft
                     where ft.BILL_CYCLE_ID = p_nbillcycleid
                       and ft.BILL_CYCLE_STEP = 4
                       and ft.TRANS_NO = AGREEMENT_INSTALLMENTS.TRANS_NO
                       and ft.CUSTKEY = AGREEMENT_INSTALLMENTS.CUSTKEY)
        and (select AGRM_SCHEDULE
               from CUST_AGREEM agrm
              where agrm.AGRM_NO = AGREEMENT_INSTALLMENTS.AGRM_NO
                and agrm.CUSTKEY = AGREEMENT_INSTALLMENTS.CUSTKEY ) = 1;

     update AGREEMENT_INSTALLMENTS
        set CHARGE_DATE = to_date(sysdate,'DD-MON-YYYY')
      where CUSTKEY = p_strcustkey
        and exists (select 1
                      from F_TRANS ft
                     where ft.BILL_CYCLE_ID = p_nbillcycleid
                       and ft.BILL_CYCLE_STEP = 4
                       and ft.TRANS_NO = AGREEMENT_INSTALLMENTS.TRANS_NO
                       and ft.CUSTKEY = AGREEMENT_INSTALLMENTS.CUSTKEY)
        and exists (select 1
                      from CUST_AGREEM agrm
                     where agrm.AGRM_NO = AGREEMENT_INSTALLMENTS.agrm_no
                       and agrm.CUSTKEY = AGREEMENT_INSTALLMENTS.custkey
                       and agrm.AGRM_SCHEDULE = 1);

     update AGREEMENT_INSTALLMENTS
        set TRANS_NO = 1
      where CUSTKEY = p_strcustkey
        and exists( select 1 from F_TRANS FT
                     where FT.BILL_CYCLE_ID =  p_nbillcycleid
                       and FT.BILL_CYCLE_STEP = 4
                       and FT.TRANS_NO = AGREEMENT_INSTALLMENTS.TRANS_NO
                       and FT.CUSTKEY = AGREEMENT_INSTALLMENTS.CUSTKEY);

     delete from F_TRANS
      where CUSTKEY = p_strcustkey
        and BILL_CYCLE_ID =  p_nbillcycleid
        and BILL_CYCLE_STEP = 4;

     /* Reverse Step 3 */
     delete from F_TRANS
      where CUSTKEY = p_strcustkey
        and BILL_CYCLE_ID = p_nbillcycleid
        and BILL_CYCLE_STEP = 3;

     update F_TRANS
        set REV_CYCLE_ID = null
      where CUSTKEY = p_strcustkey
        and REV_CYCLE_ID = p_nbillcycleid
        and BILL_CYCLE_ID <> p_nbillcycleid;

     update METR_RDG
        set IS_INVOICD = 0,
            DATE_INVCD = null,
            PROP_REF = null,
            CONN_NO = null,
            BILLGROUP = null,
            CUSTKEY = null,
            UPD_CYCLE_ID = null,
            WTRANS_NO = null,
            STRANS_NO = null,
            ETRANS_NO = null
      where CUSTKEY = p_strcustkey
        and UPD_CYCLE_ID = p_nbillcycleid;

     commit;
   exception
      when OTHERS then rollback;
      raise_application_error (-20994,'Error reversing billing steps for specfied customer');
   end;

  PROCEDURE BillIndividual (p_strCustkey      NVARCHAR2,
                            p_nbillcycleid    NUMBER,
                            p_strcurrentuser  NVARCHAR2)
  as
    strBillgroup nvarchar2(10);
    dBilngDate date;
    nProgressID number(10);

    strSQL      varchar2(4000);
    nCNT        number(10);
    strBilngDate varchar2(12);
    nSettingStm  number(10);
    strClBal    varchar2(400);

  begin

     select max(PROGRESS_ID) + 1 into nProgressID from PROCESS_PROGRESS;
     PROGRESS.PROCESS_CREATE (nProgressID);
     select BILLGROUP, BILNG_DATE into strBillgroup, dBilngDate from SUM_BCYC where CYCLE_ID = p_nbillcycleid;
     invoicesforconsumption (p_nbillcycleid, nProgressID, p_strcurrentuser, p_strCustkey);
     produceinstalments (strBillgroup, p_nbillcycleid, dBilngDate, nProgressID, p_strCustkey);
     generatestatements (strBillgroup, dBilngDate, p_nbillcycleid, '99', p_strcurrentuser, nProgressID, p_strCustkey);

          /* Update Statement Delivery with new values */
     begin
       select nvl(KEYWORD_VALUE,0) into nSettingStm
         from SETTINGS
        where KEYWORD = 'BIL_STATEMENTS_CLBLNCE_CALC';
      exception
       when OTHERS then
         nSettingStm:= 0;
      end;

      if nSettingStm = 1 then
       strClBal := 'S.INSTALMENT + S.CUR_CHARGES + S.RCPT_CHARGE1 + S.RCPT_CHARGE2 + S.RCPT_CHARGE3 + S.RCPT_CHARGE4 + S.RCPT_CHARGE5 - OP_BLNCE_V - CL_BLNCE_V';
      else
       strClBal := 'S.INSTALMENT + S.CUR_CHARGES + S.RCPT_CHARGE1 + S.RCPT_CHARGE2 + S.RCPT_CHARGE3 + S.RCPT_CHARGE4 + S.RCPT_CHARGE5';
      end if;

     select to_char(S.BILNG_DATE,'DD-MM-YYYY'), S.BILLGROUP
       into strBilngDate, strBillgroup
       from SUM_BCYC S
      where S.CYCLE_ID = p_nbillcycleid;

     select count(*)
       into nCNT
       from SYS.ALL_TABLES
      where OWNER = 'HCS_EDAMS'
       and TABLE_NAME = 'STATM_DELIVERY_' || strBillgroup;

      if (nCNT <> 0) then
         strSQL := 'select count(*) from STATM_DELIVERY_' || strBillgroup || ' where CUSTKEY = ''' || p_strCustkey || '''' || ' and BILNG_DATE = to_date(''' || strBilngDate || ''',''DD-MM-YYYY'')';
         execute immediate strSQL INTO nCNT;

         if (nCNT > 0) then

            strSQL := 'UPDATE STATM_DELIVERY_' || strBillgroup
            || ' SET (PAYMENT_NO, PAY_BY, AGE_CUR, AGE_30, AGE_60, AGE_90, AGE_120, AGE_150, AGE_180, AGE_1YR, AGE_2YR, RCPT_CHARGE1, RCPT_CHARGE2, RCPT_CHARGE3, RCPT_CHARGE4, RCPT_CHARGE5,  CL_BLNCE) = '
            || '(select  S.PAYMENT_NO, S.PAY_BY, S.AGE_CUR, S.AGE_30, S.AGE_60, S.AGE_90, S.AGE_120, S.AGE_150, S.AGE_180, S.AGE_1YR, S.AGE_2YR,S.RCPT_CHARGE1, S.RCPT_CHARGE2, S.RCPT_CHARGE3, S.RCPT_CHARGE4, S.RCPT_CHARGE5,'
            ||           strClBal
            || '   from F_STATM S where S.CUSTKEY = ''' || p_strCustkey || '''' || ' and S.BILNG_DATE = to_date(''' || strBilngDate || ''',''DD-MM-YYYY'')' || ' and S.SUBSTM_NO = 1)'
            || ' where CUSTKEY = ''' || p_strCustkey || '''' || ' AND BILNG_DATE = to_date(''' || strBilngDate || ''',''DD-MM-YYYY'')';

            execute immediate strSQL;
         end if;

      end if;
      /* End of Statement delivery update */

     PROGRESS.PROCESS_DELETE (nProgressID);

  exception
   when OTHERS then
      raise_application_error (-20994,'Error running billing steps for specfied customer');
  end;

END billing_steps;
/