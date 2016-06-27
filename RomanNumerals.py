

def int_to_roman(num: int):
    symbols = {
        1000: "M",
            900: "CM",
        500: "D",
            400: "CD",
        100: "C",
            90: "XC",
        50: "L",
            40: "XL",
        10: "X",
            9: "IX",
        5: "V",
            4: "IV",
        1: "I"
    }

    string = ""

    for value in reversed(sorted(symbols.keys())):
        reps = num // value
        string += symbols[value] * reps
        num -= value * reps

    return string


if __name__ == "__main__":
    print(int_to_roman(1996))
    print(int_to_roman(94))
    print(int_to_roman(98))
    print(int_to_roman(49))