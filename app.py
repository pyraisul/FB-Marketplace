from flask import Flask, request, render_template
import asyncio
from scraper import scrape_listings

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        query = request.form.get('query')
        max_items = int(request.form.get('max_items', 50))
        if max_items > 100:
            max_items = 100  # Limit to prevent abuse
        # Run the scraper
        listings = asyncio.run(scrape_listings(query, max_items))
        return render_template('results.html', listings=listings, query=query)
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
