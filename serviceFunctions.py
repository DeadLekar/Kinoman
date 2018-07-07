rus_letters = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
lat_letters = "abcdefghijklmnopqrstuvwxyz"
digits = "1234567890"
puncts = " .,-:;?!()[]{}="


def clear_string(str_to_clear, legitimate_symbols):
    i = 0
    new_str = ""
    while i <= len(str_to_clear)-1:
        cr_symb = str(str_to_clear[i].lower())
        if legitimate_symbols.find(cr_symb) != -1:
            new_str += str_to_clear[i]
        i += 1
    return new_str