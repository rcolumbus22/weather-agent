from prefect import flow, get_run_logger
import requests
import openai
import yagmail
from datetime import datetime
import os

# ------------ Step 1: Get Forecast Data ------------
def get_hourly_forecast(zip_code="30308"):
    print("Fetching weather data...")
    api_key = os.getenv('OPENWEATHER_API_KEY', '3c8a1b0360cafd7f2fd01c1f888dff53')
    url = f"http://api.openweathermap.org/data/2.5/forecast?zip={zip_code},us&appid={api_key}&units=imperial"
    r = requests.get(url)
    data = r.json()
    print("Weather data fetched.")

    forecast_data = []
    seen_hours = set()

    print("\nProcessing forecast data:")
    for entry in data['list']:
        forecast_time = datetime.fromtimestamp(entry['dt'])
        hour = forecast_time.hour
        date_str = forecast_time.strftime('%Y-%m-%d %H:%M')

        if hour < 6 or hour > 21:
            print(f"Skipping {date_str} - outside 6 AM to 9 PM range")
            continue

        if hour in seen_hours:
            print(f"Skipping {date_str} - duplicate hour")
            continue
        seen_hours.add(hour)

        time_label = forecast_time.strftime('%I %p').lstrip('0')
        temp = round(entry['main']['temp'])
        pop = int(entry.get('pop', 0) * 100)
        description = entry['weather'][0]['description']

        forecast_data.append({
            "time": time_label,
            "temp": f"{temp}°F",
            "pop": f"{pop}%",
            "description": description,
            "hour": hour,
            "date": date_str
        })
        print(f"Added forecast for {date_str}: {time_label}, {temp}°F, {pop}% chance of rain, {description}")

    forecast_data.sort(key=lambda x: x['hour'])
    print(f"\nForecast data processed: {len(forecast_data)} entries.")
    return forecast_data

# ------------ Step 2: Summarize with GPT ------------
def summarize_forecast_gpt(forecast_data):
    print("Summarizing forecast with GPT...")
    try:
        client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

        hourly_summary = "\n".join([
            f"{f['time']}: {f['temp']}, {f['pop']} rain, {f['description']}"
            for f in forecast_data
        ])

        commute_conditions = [f for f in forecast_data if f['hour'] in [6, 7, 17, 18]]
        commute_summary = "\n".join([
            f"{f['time']}: {f['description']}, {f['temp']}, {f['pop']} rain"
            for f in commute_conditions
        ])

        prompt = f"""Summarize this weather forecast for Atlanta (30308) in 4 sentences:
1. Overall weather trend
2. Best time for outdoor activities
3. Morning commute (6:30-8am) conditions
4. Evening commute (5-7pm) conditions

Forecast:
{hourly_summary}

Commute hours:
{commute_summary}"""

        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=300
        )

        print("GPT summary complete.")
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error occurred: {e}")
        raise

# ------------ Step 3: Format Email ------------
def get_weather_emoji(description):
    desc = description.lower()
    if "clear" in desc:
        return "☀️"
    elif "cloud" in desc:
        return "☁️"
    elif "rain" in desc:
        return "🌧️"
    elif "storm" in desc:
        return "⛈️"
    elif "snow" in desc:
        return "❄️"
    elif "fog" in desc or "mist" in desc:
        return "🌫️"
    else:
        return "🌡️"

def format_email(summary_text, forecast_data):
    print("Formatting email...")
    bullet_section = "\n".join([
        f"- **{f['time']}**: {f['temp']}, {f['pop']} chance of rain {get_weather_emoji(f['description'])}, {f['description']}"
        for f in forecast_data
    ])
    print("Email formatted.")
    return f"""{summary_text}

---

**Hourly Forecast (6 AM – 9 PM):**

{bullet_section}
"""

# ------------ Step 4: Send Email ------------
def send_email(formatted_message):
    print("Sending email...")
    try:
        email_user = os.getenv('EMAIL_USER', 'rcolumbus22@gmail.com')
        email_password = os.getenv('EMAIL_PASSWORD', 'vzuv asiv ryeu sbev')
        yag = yagmail.SMTP(email_user, email_password)
        
        today = datetime.today().strftime("%B %d, %Y")
        subject = f"Weather Update {today}"
        
        recipients = os.getenv('EMAIL_RECIPIENTS', 'rcolumbus22@gmail.com,tjh3u@virginia.edu').split(',')
        yag.send(to=recipients, subject=subject, contents=formatted_message)
        print("Email sent!")
    except Exception as e:
        print(f"Failed to send email: {e}")
        raise

# ------------ Main Flow ------------
@flow(log_prints=True)
def run_weather_agent():
    try:
        logger = get_run_logger()
        logger.info("Fetching forecast...")
        forecast_data = get_hourly_forecast()
        print(f"Forecast data: {forecast_data}")
        
        logger.info("Generating summary...")
        summary_text = summarize_forecast_gpt(forecast_data)
        print(f"Summary text: {summary_text}")

        logger.info("Formatting and sending email...")
        email_body = format_email(summary_text, forecast_data)
        print(f"Email body: {email_body}")

        send_email(email_body)
        print("✅ Weather summary sent!")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        import traceback
        traceback.print_exc()

# This allows local testing
if __name__ == "__main__":
    run_weather_agent()