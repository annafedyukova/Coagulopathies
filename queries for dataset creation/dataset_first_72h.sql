CREATE OR REPLACE TABLE `learned-vortex-290901.Coagulation.cohort_first_72h` as 
select * from (
with cohort as
    --patients who got LMWH or SC Heparin within 24 hours at least once
    (  
        select icustays.stay_id, icustays.hadm_id, icustays.intime
        from `physionet-data.mimic_icu.icustays`  icustays
        inner join `physionet-data.mimic_hosp.emar` emar --barcode scanning of medications at the time of administration.
        on icustays.hadm_id=emar.hadm_id
        inner join  `physionet-data.mimic_hosp.pharmacy` pharmacy --Formulary, dosing, and other information for prescribed medications.
        on pharmacy.pharmacy_id=emar.pharmacy_id
        where emar.event_txt = 'Administered'
        and emar.scheduletime>=icustays.intime and emar.scheduletime<=datetime_add(icustays.intime, interval 24 hour)
        and (
           upper(emar.medication) like '%ENOXAPARIN%'
           or
          (upper(emar.medication) like '%HEPARIN%' and pharmacy.route = 'SC')
            )
        --No warfarin in 48 hours prior to ICU
        and icustays.stay_id not in (select icustays_2.stay_id
                from `physionet-data.mimic_hosp.emar` emar_2
                inner join `physionet-data.mimic_icu.icustays`  icustays_2
                on emar_2.hadm_id = icustays_2.hadm_id 
                where upper(emar_2.medication)  like 'WARFARIN%' or upper(emar_2.medication)  like 'COUMADIN%'  --Warfarin, sold under the brand name Coumadin)
                and emar_2.event_txt = 'Administered'
                and emar_2.scheduletime>= datetime_sub(icustays_2.intime, interval 48 hour) and emar_2.scheduletime<=icustays_2.intime
                group by icustays_2.stay_id)
        --No intravenous heparin in 48 hours prior to ICU
        and icustays.stay_id not in (select icustays_2.stay_id
                from `physionet-data.mimic_hosp.emar` emar_2
                inner join `physionet-data.mimic_icu.icustays`  icustays_2
                on emar_2.hadm_id = icustays_2.hadm_id 
                left join  `physionet-data.mimic_hosp.pharmacy` pharmacy --Formulary, dosing, and other information for prescribed medications.
                on pharmacy.pharmacy_id=emar_2.pharmacy_id
                where upper(emar_2.medication)  like 'HEPARIN%' 
                and emar_2.event_txt = 'Administered'
                and pharmacy.route not in ('IV', 'IV BOLUS')
                and emar_2.scheduletime>= datetime_sub(icustays_2.intime, interval 48 hour) and emar_2.scheduletime<=icustays_2.intime
                group by icustays_2.stay_id)
        --INR < 2 if tested within 48 hours of ICU admission
        and icustays.stay_id not in (select icustays_2.stay_id
                from  `physionet-data.mimic_icu.icustays`  icustays_2
                inner join `physionet-data.mimic_derived.coagulation` coagulation
                on coagulation.hadm_id=icustays_2.hadm_id
                and coagulation.charttime >= icustays_2.intime
                and coagulation.charttime <= datetime_add(icustays_2.intime, INTERVAL 48 HOUR)
                and coagulation.inr < 2
                where coagulation.inr is not null)
        group by icustays.stay_id, icustays.hadm_id, icustays.intime        
    )
--blood gases
    select cohort.stay_id,cohort.intime,bg.charttime,'ph' as event, bg.ph event_value
    from  cohort
    inner join `physionet-data.mimic_derived.bg` bg
    on bg.hadm_id=cohort.hadm_id
    and bg.charttime >= cohort.intime
    and bg.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where bg.ph is not null
union all 
    select cohort.stay_id,cohort.intime,bg.charttime,'baseexcess' as event, bg.baseexcess as event_value
    from  cohort
    inner join `physionet-data.mimic_derived.bg` bg
    on bg.hadm_id=cohort.hadm_id
    and bg.charttime >= cohort.intime
    and bg.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where  bg.baseexcess is not null
union all 
    select cohort.stay_id,cohort.intime,bg.charttime,'lactate' as event, bg.lactate as event_value
    from  cohort
    inner join `physionet-data.mimic_derived.bg` bg
    on bg.hadm_id=cohort.hadm_id
    and bg.charttime >= cohort.intime
    and bg.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where  bg.lactate is not null
union all 
--coagulation
    select cohort.stay_id, cohort.intime, coagulation.charttime, 'inr' as event, coagulation.inr as event_value
    from  cohort
    inner join `physionet-data.mimic_derived.coagulation` coagulation
    on coagulation.hadm_id=cohort.hadm_id
    and coagulation.charttime >= cohort.intime
    and coagulation.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where coagulation.inr is not null
union all 
    select cohort.stay_id, cohort.intime, coagulation.charttime,'pt' as event, coagulation.pt as event_value
    from  cohort 
    inner join `physionet-data.mimic_derived.coagulation` coagulation
    on coagulation.hadm_id=cohort.hadm_id
    and coagulation.charttime >= cohort.intime
    and coagulation.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where coagulation.pt is not null
union all 
    select cohort.stay_id,cohort.intime, coagulation.charttime,'ptt' as event, coagulation.ptt  as event_value
    from  cohort
    inner join `physionet-data.mimic_derived.coagulation` coagulation
    on coagulation.hadm_id=cohort.hadm_id
    and coagulation.charttime >= cohort.intime
    and coagulation.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where coagulation.ptt is not null
union all 
--chemistry
    select cohort.stay_id, cohort.intime, chemistry.charttime, 'albumin' as event, chemistry.albumin as event_value
    from cohort
    inner join `physionet-data.mimic_derived.chemistry` chemistry
    on chemistry.hadm_id=cohort.hadm_id
    and chemistry.charttime >= cohort.intime
    and chemistry.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where chemistry.albumin is not null
union all 
    select cohort.stay_id,cohort.intime, chemistry.charttime, 'creatinine' as event, chemistry.creatinine as event_value
    from cohort
    inner join `physionet-data.mimic_derived.chemistry` chemistry
    on chemistry.hadm_id=cohort.hadm_id
    and chemistry.charttime >= cohort.intime
    and chemistry.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where chemistry.creatinine is not null
union all
--SOFA
    select cohort.stay_id, cohort.intime, sofa_.endtime, 'sofa' as event, sofa_.sofa_24hours as event_value
    from cohort
    inner join `physionet-data.mimic_derived.sofa` sofa_
    on sofa_.stay_id=cohort.stay_id
    and sofa_.endtime >= cohort.intime
    and sofa_.endtime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where sofa_.sofa_24hours is not null
--enzyme
union all
    select cohort.stay_id, cohort.intime, enzyme.charttime, 'alt' as event, enzyme.alt as event_value
    from cohort
    inner join `physionet-data.mimic_derived.enzyme` enzyme
    on enzyme.hadm_id=cohort.hadm_id
    and enzyme.charttime >= cohort.intime
    and enzyme.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where enzyme.alt is not null
union all
    select cohort.stay_id, cohort.intime, enzyme.charttime, 'ast' as event, enzyme.ast as event_value
    from cohort
    inner join `physionet-data.mimic_derived.enzyme` enzyme
    on enzyme.hadm_id=cohort.hadm_id
    and enzyme.charttime >= cohort.intime
    and enzyme.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where enzyme.ast is not null
union all
--vs
    select cohort.stay_id, cohort.intime, vitalsign.charttime, 'heart_rate' as event, vitalsign.heart_rate as event_value
    from cohort
    inner join `physionet-data.mimic_derived.vitalsign` vitalsign
    on vitalsign.stay_id=cohort.stay_id
    and vitalsign.charttime >= cohort.intime
    and vitalsign.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where vitalsign.heart_rate is not null
union all
--Either directly recorded or calculated (Systolic blood pressure + 2 * Diastolic blood pressure) / 3
    select cohort.stay_id, cohort.intime, vitalsign.charttime, 'mbp' as event, COALESCE(vitalsign.mbp , round(case when vitalsign.sbp is not null and vitalsign.dbp is not null then (vitalsign.sbp + 2 * vitalsign.dbp)/3 end,1)) as event_value
    from cohort
    inner join `physionet-data.mimic_derived.vitalsign` vitalsign
    on vitalsign.stay_id=cohort.stay_id
    and vitalsign.charttime >= cohort.intime
    and vitalsign.charttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where (vitalsign.mbp is not null or (vitalsign.dbp is not null and vitalsign.sbp is not null))
union all
--dobutamine
    select cohort.stay_id, cohort.intime, dobutamine.starttime, 'dobutamine' as event, dobutamine.vaso_amount as event_value
    from cohort
    inner join `physionet-data.mimic_derived.dobutamine` dobutamine
    on dobutamine.stay_id=cohort.stay_id
    and dobutamine.starttime >= cohort.intime
    and dobutamine.starttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where dobutamine.vaso_amount is not null
union all
--dopamine
    select cohort.stay_id, cohort.intime, dopamine.starttime, 'dopamine' as event, dopamine.vaso_amount as event_value
    from cohort
    inner join `physionet-data.mimic_derived.dopamine` dopamine
    on dopamine.stay_id=cohort.stay_id
    and dopamine.starttime >= cohort.intime
    and dopamine.starttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where dopamine.vaso_amount is not null
union all
--epinephrine
    select cohort.stay_id, cohort.intime, epinephrine.starttime, 'epinephrine' as event, epinephrine.vaso_amount as event_value
    from cohort
    inner join `physionet-data.mimic_derived.epinephrine` epinephrine
    on epinephrine.stay_id=cohort.stay_id
    and epinephrine.starttime >= cohort.intime
    and epinephrine.starttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where epinephrine.vaso_amount is not null
union all
--norepinephrine
    select cohort.stay_id, cohort.intime, norepinephrine.starttime, 'norepinephrine' as event, norepinephrine.vaso_amount as event_value
    from cohort
    inner join `physionet-data.mimic_derived.norepinephrine` norepinephrine
    on norepinephrine.stay_id=cohort.stay_id
    and norepinephrine.starttime >= cohort.intime
    and norepinephrine.starttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where norepinephrine.vaso_amount is not null
union all
--phenylephrine
    select cohort.stay_id, cohort.intime, phenylephrine.starttime, 'phenylephrine' as event, phenylephrine.vaso_amount as event_value
    from cohort
    inner join `physionet-data.mimic_derived.phenylephrine` phenylephrine
    on phenylephrine.stay_id=cohort.stay_id
    and phenylephrine.starttime >= cohort.intime
    and phenylephrine.starttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where phenylephrine.vaso_amount is not null
union all
--vasopressin
    select cohort.stay_id, cohort.intime, vasopressin.starttime, 'vasopressin' as event, vasopressin.vaso_amount as event_value
    from cohort
    inner join `physionet-data.mimic_derived.vasopressin` vasopressin
    on vasopressin.stay_id=cohort.stay_id
    and vasopressin.starttime >= cohort.intime
    and vasopressin.starttime <= datetime_add(cohort.intime, INTERVAL 72 HOUR)
    where vasopressin.vaso_amount is not null
)
order by stay_id, intime, charttime