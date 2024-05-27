package fr.formation.api_insert_data.response;

public class EventsDetailResponse {
    private String date;
    private Float Bz;
    private Float avgDeltaX;


    public String getDate() {
        return this.date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Float getBz() {
        return this.Bz;
    }

    public void setBz(Float Bz) {
        this.Bz = Bz;
    }

    public Float getAvgDeltaX() {
        return this.avgDeltaX;
    }

    public void setAvgDeltaX(Float avgDeltaX) {
        this.avgDeltaX = avgDeltaX;
    }

}
