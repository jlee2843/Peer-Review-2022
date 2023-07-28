import distance
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from Levenshtein import ratio
import spacy
nlp = spacy.load("en_core_web_lg")

from sentence_transformers import SentenceTransformer
model=SentenceTransformer('paraphrase-MiniLM-L6-v2')
from scipy.spatial.distance import cosine as cos_dist
# Based on the paper "Comparing Published Scientific Journal Articles to Their Pre-Print Versions"

def length_sim(s1, s2):
    l1 = len(s1)
    l2 = len(s2)
    longer = max(l1, l2)
    score = 1 - abs(l1-l2)/longer
    return score


def jaccard_sim(s1, s2):
    return 1 - distance.jaccard(s1, s2)

def sorensen_sim(s1, s2):
    return 1 - distance.sorensen(s1, s2)

def levenshtein_sim(s1, s2):
    return ratio(s1, s2)

# it's not entirely obvious how the cosine similarity was done

def cosine_sim(s1, s2):
    # the article is using stopword removal, Porter stemming, then scikit-learn pairwise cosine similarity
    # no Porter stemming here, just using CountVectorizer with English stopwords
    vect1 = CountVectorizer(stop_words='english')
    
    x1 = vect1.fit_transform([s1,s2])

    # vect2 = CountVectorizer(stop_words='english')

    # x2 = vect2.fit_transform([s2])

    return cosine_similarity(x1[0], x1[1])
    # return x1



def new_sim1(s1, s2):
    e1 = model.encode(s1)
    e2 = model.encode(s2)
    return 1 - cos_dist(e1, e2)

def ent_sim(s1, s2):
    n1 = nlp(s1)
    n2 = nlp(s2)
    ent1_num = [ent.text for ent in n1.ents if ent.label_ in ['CARDINAL','PERCENT']]
    ent2_num = [ent.text for ent in n2.ents if ent.label_ in ['CARDINAL','PERCENT']]

    ## intersection over union metric
    ent1_num_set = set(ent1_num)
    ent2_num_set = set(ent2_num)
    try:
        intersection_union = len(ent1_num_set.intersection(ent2_num_set)) / len(ent1_num_set.union(ent2_num_set))
        num_sim = intersection_union
    except ZeroDivisionError:
        num_sim = None
    
    ent1 = [ent.text for ent in n1.ents]
    ent2 = [ent.text for ent in n2.ents]
    ent1_set = set(ent1)
    ent2_set = set(ent2)
    try:
        intersection_union = len(ent1_set.intersection(ent2_set)) / len(ent1_set.union(ent2_set))
        ent_sim = intersection_union
    except ZeroDivisionError:
        ent_sim = None
    return num_sim, ent_sim

    


s1 = "this is the first sentence"
s2 = "this is the second sentence"

# data to test metrics on

pairs = [
    {
        'description' : "Test 1: near identical",
        'pre' : 'This is the first part of a testing pair, where the second part will be almost identical.',
        'pub' : 'This is the second part of a testing pair, where the first part was almost identical.'
    },
    {
        'description': "title of first COVID NMA study 10.1101/2020.01.30.20019844",
        'pre' : "The impact of transmission control measures during the first 50 days of the COVID-19 epidemic in China",
        'pub' : "An investigation of transmission control measures during the first 50 days of the COVID-19 epidemic in China"

    },
    {
        'description' : "abstract of first COVID NMA study 10.1101/2020.01.30.20019844",
        'pre' : "Respiratory illness caused by a novel coronavirus (COVID-19) appeared in China during December 2019. Attempting to contain infection, China banned travel to and from Wuhan city on 23 January and implemented a national emergency response. Here we evaluate the spread and control of the epidemic based on a unique synthesis of data including case reports, human movement and public health interventions. The Wuhan shutdown slowed the dispersal of infection to other cities by an estimated 2.91 days (95%CI: 2.54-3.29), delaying epidemic growth elsewhere in China. Other cities that implemented control measures pre-emptively reported 33.3% (11.1-44.4%) fewer cases in the first week of their outbreaks (13.0; 7.1-18.8) compared with cities that started control later (20.6; 14.5-26.8). Among interventions investigated here, the most effective were suspending intra-city public transport, closing entertainment venues and banning public gatherings. The national emergency response delayed the growth and limited the size of the COVID-19 epidemic and, by 19 February (day 50), had averted hundreds of thousands of cases across China.",
        'pub' : "By 23 January 2020, China had imposed a national emergency response to restrict travel and impose social distancing measures on its populace in an attempt to inhibit the transmission of severe acute respiratory syndrome–coronavirus 2 (SARS-CoV-2). However, which measures were most effective is uncertain. Tian et al. performed a quantitative analysis of the impact of control measures between 31 December 2019 and 19 February 2020, which encompasses the Lunar New Year period when millions of people traveled across China for family visits. Travel restrictions in and out of Wuhan were too late to prevent the spread of the virus to 262 cities within 28 days. However, the epidemic peaked in Hubei province on 4 February 2020, indicating that measures such as closing citywide public transport and entertainment venues and banning public gatherings combined to avert hundreds of thousands of cases of infection. It is unlikely that this decline happened because the supply of susceptible, people was exhausted, so relaxing control measures could lead to a resurgence."
    },
    {
        'description' : "COVID NMA 'Association between 2019-nCoV transmission and N95 respirator use' | https://www.medrxiv.org/content/10.1101/2020.02.18.20021881v1.full-text",
        'pre' : """Cases of a novel type of contagious pneumonia were first discovered a month ago in Wuhan. The Centers for Disease Control and Prevention (CDC) and Chinese health authorities have determined and announced that a novel coronavirus (CoV), denoted as 2019-nCoV, had caused this pneumonia outbreak.1,2 Existing evidence have confirmed the human-to-human transmission of 2019-nCoV.3

We retrospectively collected infection data from 2 January to 22 January at six departments (Respiratory, ICU, Infectious Disease, Hepatobiliary Pancreatic Surgery, Trauma and Microsurgery, and Urology) from Zhongnan Hospital of Wuhan University. Medical staffs in these department follow differential routines of occupational protection: the medical staff in departments of Respiratory, ICU, and Infectious Disease wore N95 respirators, disinfected and clean hands frequently (N95 group); due to people were not enough for the knowledge of the 2019-nCoV in these early days of the pneumonia outbreak, the medical staff in the other three departments wore no medical masks, disinfected and clean hands occasionally (no-mask group). Suspicious cases of 2019-nCoV infection was diagnosed with chest CT and confirmed with molecular diagnosis. In total, 28/58 ([confirmed/suspicious]) 2019-nCoV patients have been diagnosed during the collection period. Patient exposure was significantly higher for the N95 group compared to no-mask group (Table 1) (For confirmed patients: difference: 733%; exposure odds ratio: 8.33)

Among 493 medical staffs, zero out of 278 (([doctors+nurse] 56+222) from the N95 group were infected by 2019-nCoV. In stark contrast, 10 out of 213 (77+136) from the no-mask group were confirmed infected (Table 1). Regardless of significantly lowered exposure, the 2019-nCoV infection rate for medical staff was significantly increased in the no-mask group compared to the N95 respirator group (difference: 4.65%, [95% CI: 1.75%-infinite]; P<2.2e-16) (adjusted odds ratio (OR): 464.82, [95% CI: 97.73-infinite]; P<2.2e-16).

Meanwhile, we analyzed the medical staff infection data from Huangmei People’ s Hospital (12 confirmed patients) and Qichun People’ s Hospital (11 confirmed patients) and have observed the similar phenomenon. None of medical staff wearing the N95 respirators and follow frequent routines of disinfection with hand washing were infected by 2019-nCoV.

A randomized clinical trial has reported N95 respirators vs medical masks as worn by participants in this trial resulted in no significant difference in the incidence of laboratory confirmed influenza.4 In our study, we found N95 respirators, disinfection and hand washing can help to reduce the risk of 2019-nCoV infection in medical staffs. Interestingly, department with a high proportion of male doctors seem to have a higher risk of infection. Our results call for re-emphasizing strict occupational protection code in battling this novel contagious disease.

The risk of 2019-nCoV infection was higher in the open area than in the quarantined area. N95 may be more effective for 2019-nCoV infections.""",
        'pub' : """Cases of a novel type of contagious pneumonia were first reported in December 2019 in Wuhan, China. The Centers for Disease Control and Prevention (CDC) and Chinese health authorities have determined that a novel coronavirus (CoV), denoted as 2019-nCoV (SARS-CoV-2), is the cause of this pneumonia outbreak (COVID-19) [[1]
,[2]
]. Existing evidence has confirmed the human-to-human transmission of 2019-nCoV [[3]
].
We retrospectively collected infection data from 2 to 22 January 2020 at six departments (Respiratory, Intensive Care Unit (ICU), Infectious Disease, Hepatobiliary Pancreatic Surgery, Trauma and Microsurgery and Urology) from Zhongnan Hospital of Wuhan University. Medical staff (doctors and nurses) followed differential routines of occupational protection: (1) staff at the Departments of Respiratory Medicine, ICU, and Infectious Disease (mainly quarantined area) wore N95 respirators, and disinfected and cleaned their hands frequently (the N95 group); (2) medical staff in the other three departments wore no medical masks, and disinfected and cleaned their hands only occasionally (the no-mask group). The difference was because the latter departments were not considered to be high risk in the early days of the outbreak.
Suspected cases of 2019-nCoV infection were investigated by chest computed tomography, and confirmed by molecular diagnosis. In total, 28 and 58 patients had confirmed and suspected 2019-nCoV-infection, respectively. Patient exposure was significantly higher for the N95 group compared with the no-mask group (for confirmed patients, difference: 733%; exposure odds ratio: 8.33, Table I).
Among the 493 medical staff, none of the 278 staff (56 doctors and 222 nurses) in the N95 group became infected, but 10 of 213 staff (77 doctors and 136 nurses) from the no-mask group were confirmed as infected (Table I). Regardless of their lower risk of exposure, the 2019-nCoV infection rate for medical staff was significantly increased in the no-mask group compared with the N95 respirator group (difference: 4.65%, (95% confidence interval: 1.75%–infinite); P<2.2e-16) (adjusted odds ratio: 464.82, (95% confidence interval: 97.73–infinite); P<2.2e-16).
Likewise, we analysed the medical staff infection data from Huangmei People's Hospital (12 confirmed patients) and Qichun People's Hospital (11 confirmed patients), and observed a similar phenomenon. No medical staff wearing the N95 respirators and following routines of frequent disinfection and hand washing were infected by 2019-nCoV up until 22 January 2020.
A randomized clinical trial has reported that the N95 respirators vs medical masks resulted in no significant difference in the incidence of laboratory confirmed influenza [[4]
]. In our study, we observed that the N95 respirators, disinfection and hand washing appeared to help reduce the infectious risk of 2019-nCoV in doctors and nurses. Interestingly, departments with a high proportion of male doctors seemed to have a higher risk of infection. Our results emphasize the need for strict occupational protection measures in fighting COVID-19."""
    },
    {
        'description' : "COVID NMA: Abstract of 'Clinical Features of COVID-19-Related Liver Functional Abnormality' | 10.1101/2020.02.26.20026971",
        'pre' : """BACKGROUND A recent outbreak of SARS-CoV-2 infection occurs mainly in China, with rapidly increasing the number of cases (namely COVID-19). Abnormal liver functions are frequently present in these patients, here we aimed to clarify the clinical features of COVID-19-related liver damage to provide some references for the clinical treatment.

METHODS In this retrospective, single-center study, we included all confirmed COVID-19 cases in Shanghai Public Health Clinical Center from January 20 to January 31, 2020. The outcomes were followed up until February 19, 2020. A total of 148 cases were analyzed for clinical features, laboratory parameters (including liver function tests), medications and the length of stay.

FINDINGS Of 148 confirmed SARS-CoV-2-infected patients, 49.3% were females and 50.7% were males. The median age was 50.5 years (interquartile range, 36-64). Patients had clinical manifestations of fever (70.1%), cough (45.3%), expectoration (26.7%) at admission. 75 patients (50.7%) showed abnormal liver functions at admission. Patients (n = 75) who had elevated liver function index were more likely to have a moderate-high degree fever (44% vs 27.4%; p = 0.035) and significantly present in male patients (62.67% vs 38.36%; p = 0.005). The numbers of CD4+ and CD8+ T cells were significantly lower in abnormal liver function group than those in normal liver function group. There was no statistical difference in prehospital medications between normal and abnormal liver function groups, while the utilization rate of lopinavir/ritonavir after admission was significantly higher in patients with emerging liver injury than that in patients with normal liver functions. Importantly, the emerging abnormal liver functions after admission caused a prolonged length of stay

INTERPRETATION SARS-CoV-2 may cause the liver function damage and the Lopinavir/ritonavir should be applied carefully for the treatment of COVID-19.""",
        'pub' : """Background & Aims
Some patients with SARS-CoV-2 infection have abnormal liver function. We aimed to clarify the features of COVID-19-related liver damage to provide references for clinical treatment.
Methods
We performed a retrospective, single-center study of 148 consecutive patients with confirmed COVID-19 (73 female, 75 male; mean age, 50 years) at the Shanghai Public Health Clinical Center from January 20 through January 31, 2020. Patient outcomes were followed until February 19, 2020. Patients were analyzed for clinical features, laboratory parameters (including liver function tests), medications, and length of hospital stay. Abnormal liver function was defined as increased levels of alanine and aspartate aminotransferase, gamma glutamyltransferase, alkaline phosphatase, and total bilirubin.
Results
Fifty-five patients (37.2%) had abnormal liver function at hospital admission; 14.5% of these patients had high fever (14.5%), compared with 4.3% of patients with normal liver function (P = .027). Patients with abnormal liver function were more likely to be male, and had higher levels of procalcitonin and C-reactive protein. There was no statistical difference between groups in medications taken before hospitalization; a significantly higher proportion of patients with abnormal liver function (57.8%) had received lopinavir/ritonavir after admission compared to patients with normal liver function (31.3%). Patients with abnormal liver function had longer mean hospital stays (15.09 ± 4.79 days) than patients with normal liver function (12.76 ± 4.14 days) (P = .021).
Conclusions
More than one third of patients admitted to the hospital with SARS-CoV-2 infection have abnormal liver function, and this is associated with longer hospital stay. A significantly higher proportion of patients with abnormal liver function had received lopinavir/ritonavir after admission; these drugs should be given with caution.""",
    },
    {   
        'description': "COVID NMA: Abstract of 'Therapeutic effects of dipyridamole on COVID-19 patients with coagulation dysfunction | https://www.medrxiv.org/content/10.1101/2020.02.27.20027557v1",
        'pre' : """The human coronavirus HCoV-19 infection can cause acute respiratory distress syndrome (ARDS), hypercoagulability, hypertension, extrapulmonary multiorgan dysfunction. Effective antiviral and anti-coagulation agents with safe clinical profiles are urgently needed to improve the overall prognosis. We screened an FDA approved drug library and found that an anticoagulant agent dipyridamole (DIP) suppressed HCoV-19 replication at an EC50 of 100 nM in vitro. It also elicited potent type I interferon responses and ameliorated lung pathology in a viral pneumonia model. In analysis of twelve HCoV-19 infected patients with prophylactic anti-coagulation therapy, we found that DIP supplementation was associated with significantly increased platelet and lymphocyte counts and decreased D-dimer levels in comparison to control patients. Two weeks after initiation of DIP treatment, 3 of the 6 severe cases (60%) and all 4 of the mild cases (100%) were discharged from the hospital. One critically ill patient with extremely high levels of D-dimer and lymphopenia at the time of receiving DIP passed away. All other patients were in clinical remission. In summary, HCoV-19 infected patients could potentially benefit from DIP adjunctive therapy by reducing viral replication, suppressing hypercoagulability and enhancing immune recovery. Larger scale clinical trials of DIP are needed to validate these therapeutic effects.""",
        'pub' : """Severe acute respiratory syndrome coronavirus 2 (SARS-CoV-2) infection can cause acute respiratory distress syndrome, hypercoagulability, hypertension, and multiorgan dysfunction. Effective antivirals with safe clinical profile are urgently needed to improve the overall prognosis. In an analysis of a randomly collected cohort of 124 patients with COVID-19, we found that hypercoagulability as indicated by elevated concentrations of D-dimers was associated with disease severity. By virtual screening of a U.S. FDA approved drug library, we identified an anticoagulation agent dipyridamole (DIP) in silico, which suppressed SARS-CoV-2 replication in vitro. In a proof-of-concept trial involving 31 patients with COVID-19, DIP supplementation was associated with significantly decreased concentrations of D-dimers (P < 0.05), increased lymphocyte and platelet recovery in the circulation, and markedly improved clinical outcomes in comparison to the control patients. In particular, all 8 of the DIP-treated severely ill patients showed remarkable improvement: 7 patients (87.5%) achieved clinical cure and were discharged from the hospitals while the remaining 1 patient (12.5%) was in clinical remission."""
    }
]

for pair in pairs:
    descr = pair['description']
    pre = pair['pre']
    pub = pair['pub']
    print(descr, pre, pub)
    pair['length similarity'] = length_sim(pre, pub)
    pair['jaccard similarity'] = jaccard_sim(pre, pub)
    pair['sorensen similarity'] = sorensen_sim(pre, pub)
    pair['levenshtein similarity'] = levenshtein_sim(pre, pub)
    pair['cosine'] = cosine_sim(pre, pub)
    pair['new1'] = new_sim1(pre, pub)
    pair['num'], pair['ent'] = ent_sim(pre, pub)

df = pd.DataFrame(pairs)
print(df)