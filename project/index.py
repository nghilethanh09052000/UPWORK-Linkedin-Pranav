import requests


r = requests.get(
    "https://www.linkedin.com/jobs/view/care-management-assistant-at-luminis-health-4195923975?position=25&pageNum=0&refId=o%2FifKKqFq6CbXXpiQ7gwpQ%3D%3D&trackingId=OnZD4tF2bnvOAntTqlyxAw%3D%3D"
)

with open("test.html", "w", encoding="utf-8") as f:
    f.write(r.text)