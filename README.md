# **MirrorMovie**


[***MirrorMovie - 느낌 기반 영화 추천 서비스***](https://www.mirrormovie.club)

<br>

유저 코멘트를 학습해 맥락에 어울리는 영화를 추천해 줍니다.

UI와 기능은 [넷플릭스 windows 앱](https://www.microsoft.com/ko-kr/p/netflix/9wzdncrfj3tj?activetab=pivot:overviewtab)을 벤치마킹했고, 데이터는 [네이버 영화](https://movie.naver.com/)에서 수집했습니다.

<br>
<br>


## **목차**

### [기능](#기능)
  1. [주제별 추천](#주제별-추천)
  1. [연관 추천](#연관-추천)
  1. [검색 추천](#검색-추천)
### [엔지니어링](#엔지니어링)
  1. [Architecture](#Architecture)
  1. [Data Flow](#Data-Flow)
  1. [Model](#Model)

<br>

#

<br>
<br>

## **기능**

<br>

### **주제별 추천**

* 비슷한 영화 묶음 중 관객 평가가 좋은 영화 목록을 추천합니다.
* 영화 묶음 키워드를 함께 노출해 해당 영화 묶음이 어떤 느낌인지 알려줍니다.

![](https://user-images.githubusercontent.com/31299614/120593389-61c09f80-c47a-11eb-8618-48cf477c0c60.gif)

<br>

# 

<br>

### **연관 추천**

* 선택한 영화와 유사한 영화를 추천합니다.
* 그 영화의 배우, 감독, 작가가 참여한 다른 영화를 보여줍니다.

![](https://user-images.githubusercontent.com/31299614/120592343-a9462c00-c478-11eb-9b79-fe85b8311a96.gif)

<br>

#

<br>

### **검색 추천**

* 검색어를 입력하면 적합한 영화 목록을 추천합니다.

![](https://user-images.githubusercontent.com/31299614/120595830-f5e03600-c47d-11eb-851d-2ee955832904.gif)

<br>

[목차로 돌아가기](#목차)
#

<br>

## **엔지니어링**

<br>

### **Architecture**

<br>

![](https://user-images.githubusercontent.com/31299614/120283354-0bc2ef00-c2f6-11eb-9408-a73b619f90a0.png)

<br>

#

<br>

### **Data Flow**

<br>

![](https://user-images.githubusercontent.com/31299614/120121768-476b9500-c1e0-11eb-8e36-f9ce0a872e2f.png)

<br>

#

<br>

### **Model**

<br>

![](https://user-images.githubusercontent.com/31299614/120399284-a6631280-c376-11eb-80d6-0f5417bed01d.png)
![](https://user-images.githubusercontent.com/31299614/120400376-b4b22e00-c378-11eb-8eb4-b9b0028385be.png)
![](https://user-images.githubusercontent.com/31299614/120587354-25884180-c470-11eb-83a3-1449997658b3.png)
![](https://user-images.githubusercontent.com/31299614/120587182-e0640f80-c46f-11eb-828d-f5254ce00933.png)

<br>

[목차로 돌아가기](#목차)
#

<br>