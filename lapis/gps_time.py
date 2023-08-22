import datetime

class LidarTimestamps:
    def __init__(self, array):
        """
        Initializes an instance of the LidarTimestamps class.

        Args:
            array (numpy.ndarray): The input array containing lidar data.

        """
        self.array = array

    def _count_leaps(self, gps_time):
        """
        Count the number of leap seconds that have passed.

        Args:
            gps_time (int or float): The GPS time in seconds.

        Returns:
            int: The number of leap seconds that have passed.

        """
        leaps = (
            46828800, 78364801, 109900802, 173059203, 252028804,
            315187205, 346723206, 393984007, 425520008, 457056009,
            504489610, 551750411, 599184012, 820108813, 914803214,
            1025136015
        )

        no_leaps = 0
        for leap in leaps:
            if gps_time >= leap:
                no_leaps += 1

        return no_leaps

    def _convert_gpstime(self, gps_time):
        """
        Convert GPS time to Unix time.

        Args:
            gps_time (float): The GPS time in seconds.

        Returns:
            str: The Unix time converted from GPS time in 'YYYY-MM-DD HH:MM:SS' format.

        """
        offset = 315964800
        gps_time = float(gps_time)
        gps_time += 1e9  # Unadjusted GPS time
        unix_time = gps_time + offset - self._count_leaps(gps_time)
        datetimestr = datetime.datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')

        return datetimestr

    def get_timestamps(self):
        """
        Get unique timestamps from the lidar data.

        Returns:
            set: A set containing the unique timestamps in 'YYYY-MM-DD HH:MM:SS' format.

        """
        gpstime_index = self.array.dtype.names.index("GpsTime")  # returns the index value of the 'GpsTime' column
        timestamps = set([self._convert_gpstime(i[gpstime_index]) for i in self.array])

        return timestamps