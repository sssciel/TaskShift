import numpy as np

def msse(predicted_data, real_data, b=12):
    """
    In TaskShift model uses MSSE error for finding best model.

    This allows you to take into account not only the overall average
    prediction for both days, but also the segments included in them.
    So, Sunday will not be able to smooth out the bad forecast for Saturday.

    MSSE = 1/s * sum{i=1, n}(s_i * (l_i - L_i) ** 2)

    l_i - predicted mean value of segment i,
    L_i - real mean value of segment i,
    s - sum of all segments,
    s_i - segment. s_1 = b, s_i = b * 2 ** (i - 1), 
    default b is 12 (3 hours = 4 measurements in one hour * 3 hours)

    I advise to use b, which is a divisor of the number of predictions length!
    """

    if predicted_data.shape[0] != real_data.shape[0]:
        print("Predicted hasn't equal length with real data")
        return np.inf

    if predicted_data.shape[0] == 0:
        print("There is no predictions")
        return np.inf

    forecasts_length = predicted_data.shape[0]

    length_for_segments = forecasts_length // b
    segments = []

    while (length_for_segments > 0):
        segments.append(length_for_segments * b)
        length_for_segments >>= 1

    # Only if b is not divisor of predictions length.
    # Element will broke the order in the list, 
    # but it will not affect the algo. 
    if (segments[0] != forecasts_length):
        segments.append(forecasts_length)

    sum_of_segments = np.sum(segments)

    msse = np.sum([s * ((np.mean(predicted_data[:s]) - np.mean(real_data[:s])) ** 2) for s in segments]) / sum_of_segments

    return msse