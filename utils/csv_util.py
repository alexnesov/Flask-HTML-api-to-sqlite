def tuplesToCSV(Tuples):
    """
    To be used by Flask's Reponse class, to return a csv type
    Transform tuples int a csv style sheet
    :param 1: tuples
    :returns: a long string that mimics a CSV
    """
    reReconstructedCSV = "test"

    len_element = Tuples
    print(len_element)

    """
    for line in Tuples:
        c1 = line[0]
        c2 = line[1]
        c3 = line[2]
        c4 = str(line[3])
        c5 = str(line[4])
        c6 = str(line[5])
        c7 = str(line[6])

        reReconstructedLine = c1 + ',' + c2 + ','\
            + c3 + ',' + c4 + ',' + c5 + ',' + c6 + ',' + c7 + '\n'
        reReconstructedCSV = reReconstructedCSV + reReconstructedLine
    """

    return reReconstructedCSV
