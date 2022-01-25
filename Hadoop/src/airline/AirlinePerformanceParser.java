package airline;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {
	private int year, month;
	private int arriveDelayTime, departureDelayTime, distance = 0;
	private boolean arriveDelayAvailable, departureDelayAvailable, distanceAvailable = true;
	private String uniqueCarrier;

	public AirlinePerformanceParser(Text text) {
		try {
			String[] columns = text.toString().split(","); // 라인별로 필드 구분자를 이용하여 문자열 배열 생성
			year = Integer.parseInt(columns[0]); // 운항 연도 설정
			month = Integer.parseInt(columns[1]); // 운항 월 설정
			uniqueCarrier = columns[8]; // 항공사 코드 설정

			if (!columns[15].equals("NA")) { // 항공기 출발 지연 시간 설정
				departureDelayTime = Integer.parseInt(columns[15]);
			} else {
				departureDelayAvailable = false;
			}

			if (!columns[14].equals("NA")) { // 항공기 도착 지연 시간 설정
				arriveDelayTime = Integer.parseInt(columns[14]);
			} else {
				arriveDelayAvailable = false;
			}

			if (!columns[18].equals("NA")) { // 운항 거리 설정
				distance = Integer.parseInt(columns[18]);
			} else {
				distanceAvailable = false;
			}
		} catch (Exception e) {
			System.out.println("Error parsing a record : " + e.getMessage());
		}
	}

	public int getYear() {
		return year;
	}

	public int getMonth() {
		return month;
	}

	public int getArriveDelayTime() {
		return arriveDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}

	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}

	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}
}