import Container from '@material-ui/core/Container';
import CssBaseline from '@material-ui/core/CssBaseline';
import { makeStyles } from '@material-ui/core/styles';

const useStyles = makeStyles(theme => ({
    weatherContainer: {
    },
  }));

const Weather = () => {
    const getWeather = () => {
        var weather = new XMLHttpRequest();
        // Philadelphia latitude and longtitude with OpenWeatherMap API and access code from geekforgeeks
        // https://www.geeksforgeeks.org/weather-app-using-vanilla-javascript/
        weather.open("GET","https://api.openweathermap.org/data/2.5/weather?lat=39.952&lon=-75.164&appid=6d055e39ee237af35ca066f35474e9df",false);
        weather.send(null);
        
        var r = JSON.parse(weather.response);
        var dis = "Current location: " + r.name + "<br />";
        var temp = r.main.temp + "<br>";
        var wind = "Wind: " + r.wind.speed + " in " + r.wind.degree + " degree " + "<br>";
        document.getElementById("weather").innerHTML = dis;
        document.getElementById("temp").innerHTML = temp;
        document.getElementById("wind").innerHTML = wind;
    }

    return (
        <Container className={weatherContainer} maxWidth={'md'}>
            <CssBaseline />
            <div id = "weatherblock">
                <button onclick={getWeather()} class="btn btn-info">
                    <span class="glyphicon glyphicon-cloud"></span>Search the current weather</button> 
                <div id = "weather"></div>
                <p><span id="temp"></span></p>
                <p><span id="wind"></span></p>
		    </div>	
        </Container>
    )
}

export default Weather