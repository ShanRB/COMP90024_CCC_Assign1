
def parse_language_code(code):
    if code == 'en':
        return 'English(en)'
    elif code == 'ar':
        return 'Arabic(ar)'
    elif code == 'bn':
        return 'Bengali(bn)'
    elif code == 'cs':
        return 'Czech(cs)'
    elif code == 'da':
        return 'Danish(da)'
    elif code == 'de':
        return 'German(de)'
    elif code == 'el':
        return 'Greek(el)'
    elif code == 'es':
        return 'Spanish(es)'
    elif code == 'fa':
        return 'Persian(fa)'
    elif code == 'fi':
        return 'Finnish(fi)'
    elif code == 'fil':
        return 'Filipino(fil)'
    elif code == 'fr':
        return 'French(fr)'
    elif code == 'he':
        return 'Hebrew(he)'
    elif code == 'hi':
        return 'Hindi(hi)'
    elif code == 'hu':
        return 'Hungarian(hu)'
    elif code == 'id':
        return 'Indonesian(id)'
    elif code == 'it':
        return 'Italian(it)'
    elif code == 'ja':
        return 'Japanese(ja)'
    elif code == 'ko':
        return 'Korean(ko)'
    elif code == 'msa':
        return 'Malay(msa)'
    elif code == 'nl':
        return 'Dutch(nl)'
    elif code == 'no':
        return 'Norwegian(no)'
    elif code == 'pl':
        return 'Polish(pl)'
    elif code == 'pt':
        return 'Portuguese(pt)'
    elif code == 'ro':
        return 'Romanian(ro)'
    elif code == 'ru':
        return 'Russian(ru)'
    elif code == 'sv':
        return 'Swedish(sv)'
    elif code == 'th':
        return 'Thai(th)'
    elif code == 'tr':
        return 'Turkish(tr)'
    elif code == 'uk':
        return 'Ukrainian(uk)'
    elif code == 'ur':
        return 'Urdu(ur)'
    elif code == 'vi':
        return 'Vietnamese(vi)'
    elif code == 'zh-cn':
        return 'Chinese(Simplified)(zh-cn)'
    elif code == 'zh-tw':
        return 'Chinese(Traditional)(zh-tw)'
    else:
        return 'Others('+code+')'

def merge_dict(dict_list):
    result = dict_list[0]
    for i in range(1,len(dict_list)):
        for key,value in dict_list[i].items():
            if key not in result:
                result[key] = value
            else:
                result[key] += value
    return result
    
