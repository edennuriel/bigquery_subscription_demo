syntax = "proto3";
message ProtocolBuffer {
	string  ride_id = 1 ;
	int32   point_idx = 2 ;
	float    latitude = 3 ;
	float    longitude = 4 ;
	string  timestamp = 5 ;
	float    meter_reading = 6 ;
	float    meter_increment = 7 ;
	string  ride_status = 8 ;
	int32   passenger_count = 9 ;
}
