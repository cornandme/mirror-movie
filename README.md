# **Mirror Movie**

https://www.mirrormovie.club

***느낌 기반 영화 추천 서비스***

<br>

유저 코멘트를 학습해 맥락에 어울리는 영화를 추천해 줍니다.

UI와 기능은 [넷플릭스 windows 앱](https://www.microsoft.com/ko-kr/p/netflix/9wzdncrfj3tj?activetab=pivot:overviewtab)을 벤치마킹했고, 데이터는 [네이버 영화](https://movie.naver.com/)에서 수집했습니다.

<br>
<br>


## **목차**

### [기능](#기능)
  1. [주제별 추천](#주제별-추천)
  1. [연관 추천](#관련-영화-추천)
  1. [검색 추천](#검색-추천)
### [엔지니어링](#엔지니어링)
  1. [Architecture](#Architecture)
  1. [Data Flow](#Data-Flow)
  1. [Model](#Model)

<br>
<br>

## **기능**

<br>

### **주제별 추천**

* 비슷한 영화 묶음 중 관객 평가가 좋은 영화 목록을 추천합니다.
* 영화 묶음 키워드를 함께 노출해 해당 영화 묶음이 어떤 느낌인지 알려줍니다.

[목차로 돌아가기](#목차)
# 

<br>

### **연관 영화 추천**

* 선택한 영화와 유사한 영화를 추천합니다.
* 그 영화의 배우, 감독, 작가가 참여한 다른 영화를 보여줍니다.

[목차로 돌아가기](#목차)
#

<br>

### **검색 추천**

* 검색어를 입력하면 적합한 영화 목록을 추천합니다.

[목차로 돌아가기](#목차)
#

<br>

## **엔지니어링**

<br>

### **Architecture**

![](https://user-images.githubusercontent.com/31299614/120112834-84b92e00-c1b2-11eb-8504-c67d4d00c24f.png)

[목차로 돌아가기](#목차)
#

<br>

### **Data Flow**

![](https://user-images.githubusercontent.com/31299614/120121768-476b9500-c1e0-11eb-8e36-f9ce0a872e2f.png)

[목차로 돌아가기](#목차)
#

<br>

### **Model**

![](https://user-images.githubusercontent.com/31299614/120148987-0301e880-c224-11eb-995b-7c0558900d6e.png)
![](https://user-images.githubusercontent.com/31299614/120148316-fdf06980-c222-11eb-96e2-fb492305e412.png)
![](https://user-images.githubusercontent.com/31299614/120149735-1bbece00-c225-11eb-84a3-4322f9c9ddf7.png)

[목차로 돌아가기](#목차)
#

<br>