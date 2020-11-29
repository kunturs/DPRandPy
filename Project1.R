options(stringsAsFactors = FALSE)
library(sqldf)
library(dplyr)
library(data.table)
library(microbenchmark)

setwd("~/OneDrive - Politechnika Warszawska/2ND SEMESTER/DPRPY/project1/datasets")
Tags <- read.csv("Tags.csv.gz")
Badges <- read.csv("Badges.csv.gz")
Comments <- read.csv("Comments.csv.gz")
PostLinks <- read.csv("PostLinks.csv.gz")
Posts <- read.csv("Posts.csv.gz") 
Users <- read.csv("Users.csv.gz")
Votes <- read.csv("Votes.csv.gz")

Badges_ <- as.data.table(Badges)
Tags_ <- as.data.table(Tags)
Votes_ <- as.data.table(Votes)
Posts_ <- as.data.table(Posts)
Comments_ <- as.data.table(Comments)
PostLinks_ <- as.data.table(PostLinks)
Users_ <- as.data.table(Users)

# Task 1 SQLDF AS reference
# head(Tags)
head(PostLinks)

loadposts <- sqldf("
SELECT Posts.Title, RelatedTab.NumLinks FROM
(SELECT RelatedPostId AS PostId, COUNT(*) AS NumLinks FROM PostLinks
GROUP BY RelatedPostId) AS RelatedTab
JOIN Posts ON RelatedTab.PostId=Posts.Id WHERE Posts.PostTypeId=1
ORDER BY NumLinks DESC")


#Querie 1 using dplyr

PostsNew <- PostLinks%>%
  left_join(Posts, by=c("RelatedPostId" = "Id")) %>%
  filter(PostTypeId==1) %>%
  select(Title,RelatedPostId) %>%
  group_by(Title) %>%
  summarise(Numlinks=n()) %>%
  arrange(desc(Numlinks))

#using base function

base1 <- aggregate(PostLinks$RelatedPostId, by=list(PostLinks[["RelatedPostId"]]), FUN=length)
colnames(base1)[colnames(base1) == "Group.1"] <- "PostId"
colnames(base1)[colnames(base1) == "x"] <- "NumLinks"

merge1 <- merge(base1, Posts, by.x = "PostId", by.y = "Id")
merge12 <- merge1[merge1$PostTypeId == "1",c("Title","NumLinks"),]
merge12 <- merge12[order(merge12$NumLinks, decreasing = TRUE),]

#using data table

dt1 <- PostLinks_[, .N, by=RelatedPostId]
setnames(dt1, 'RelatedPostId', 'PostId')
setnames(dt1, 'N', 'NumLinks')

dt12 <- merge.data.table(dt1, Posts_, by.x = "PostId", by.y = "Id")
dt12 <- dt12[PostTypeId == 1 ,c("Title","NumLinks")]
dt13 <- as.data.frame(dt12[order(NumLinks, decreasing = T),])

#result microbenchmark
result1 <- microbenchmark(merge12, PostsNew, loadposts, dt13, times = 1000)
result1





sqlqueries2 <- sqldf("
                     SELECT
Users.DisplayName,
Users.Age,
Users.Location,
SUM(Posts.FavoriteCount) AS FavoriteTotal,
Posts.Title AS MostFavoriteQuestion,
MAX(Posts.FavoriteCount) AS MostFavoriteQuestionLikes
FROM Posts
JOIN Users ON Users.Id=Posts.OwnerUserId
WHERE Posts.PostTypeId=1
GROUP BY OwnerUserId
ORDER BY FavoriteTotal DESC
LIMIT 10")

sqlqueries2

#use dplyr

suppressWarnings(q <- Posts %>%
                   inner_join(Users, by=c("OwnerUserId" = "Id"))%>%
                   filter(PostTypeId==1) %>%
                   select(DisplayName, Age, Location, FavoriteCount, Title, OwnerUserId)%>%
                   group_by(DisplayName, Age, Location)%>%
                   summarise(MostFavoriteQuestionLikes = max(FavoriteCount, na.rm = TRUE), 
                             FavoriteTotal=sum(FavoriteCount, na.rm = TRUE),
                             MostFavouriteQuestion=Title[which(FavoriteCount == MostFavoriteQuestionLikes)]) %>%
                   arrange(desc(FavoriteTotal))%>%
                   head(10))

q <- Posts %>%
  inner_join(Users, by=c("OwnerUserId" = "Id"))%>%
  filter(PostTypeId==1) %>%
  select(DisplayName, Age, Location, FavoriteCount, Title, OwnerUserId)%>%
  group_by(DisplayName, Age, Location)%>%
  summarise(MostFavoriteQuestionLikes = max(FavoriteCount, na.rm = TRUE), 
         FavoriteTotal=sum(FavoriteCount, na.rm = TRUE),
         MostFavouriteQuestion=Title[which(FavoriteCount == MostFavoriteQuestionLikes)]) %>%
  arrange(desc(FavoriteTotal))%>%
  head(10)


#use base R

Summary <- aggregate(x = Posts$FavoriteCount, by = list(Posts[["OwnerUserId"]]), FUN = sum, na.rm=TRUE, na.action=NULL)
colnames(Summary)[colnames(Summary) == "x"] <- "FavoriteTotal"
colnames(Summary)[colnames(Summary) == "Group.1"] <- "OwnerUserId"
suppressWarnings(InsideMax <- aggregate(Posts$FavoriteCount, by = list(Posts[["OwnerUserId"]]), max, na.rm=TRUE, na.action=NULL))
colnames(InsideMax)[colnames(InsideMax) == "x"] <- "FavoriteCount"
colnames(InsideMax)[colnames(InsideMax) == "Group.1"] <- "OwnerUserId"
doMerge <- merge(Summary,InsideMax, by="OwnerUserId")
doMerge <- doMerge[order(doMerge$FavoriteTotal, decreasing = T),]
doMerge <- merge(doMerge, Posts, by=c("OwnerUserId","FavoriteCount"))[,c("OwnerUserId","FavoriteTotal","FavoriteCount","Title","PostTypeId"),]
doMerge <- merge(doMerge, Users, by.x = "OwnerUserId", by.y = "Id")
doMerge <- doMerge[doMerge$PostTypeId==1,]
doMerge <- doMerge[order(doMerge$FavoriteTotal,decreasing = T),]
base2 <- doMerge[1:10,c("DisplayName","Age","Location","FavoriteTotal","Title","FavoriteCount"),]
colnames(base2)[colnames(base2) == "Title"] <- "MostFavoriteQuestion"
colnames(base2)[colnames(base2) == "FavoriteCount"] <- "MostFavoriteQuestionLikes"

#use data table
suppressWarnings(Summary <- Posts_[PostTypeId==1 & !is.na(OwnerUserId),.(FavoriteTotal = sum(FavoriteCount, na.rm=TRUE), 
                                                                                FavoriteCount = max(FavoriteCount, na.rm=TRUE)),by = OwnerUserId])
Summary <- merge.data.table(Summary, Posts, by = c("OwnerUserId","FavoriteCount"))
Summary <- Summary[order(FavoriteTotal, decreasing = T),][1:10,]
dt2 <- merge.data.table(Summary, Users, by.x = "OwnerUserId", by.y = "Id")[order(FavoriteTotal,decreasing = T),
                                                                                     c("DisplayName","Age","Location","FavoriteTotal","Title","FavoriteCount"),]
setnames(dt2, "Title", "MostFavoriteQuestion")
setnames(dt2, "FavoriteCount", "MostFavoriteQuestionLikes")
dt2 <- as.data.frame(dt2)

#Result microbenchmark

Result2MicroBenchmark <- microbenchmark(sqlqueries2, q, base2, dt2, times = 1000)


sql3 <- sqldf("SELECT
Posts.Title,
CmtTotScr.CommentsTotalScore FROM (
SELECT
PostID,
UserID,
SUM(Score) AS CommentsTotalScore
FROM Comments
GROUP BY PostID, UserID ) AS CmtTotScr
JOIN Posts ON Posts.ID=CmtTotScr.PostID AND Posts.OwnerUserId=CmtTotScr.UserID WHERE Posts.PostTypeId=1
ORDER BY CmtTotScr.CommentsTotalScore DESC
LIMIT 10")

View(sql3)

dplyr3 <- Posts %>%
  inner_join(Comments, by=c("Id" = "PostId", "OwnerUserId" = "UserId")) %>%
  filter(PostTypeId==1)%>%
  #filter(OwnerUserId==UserId) %>%
  group_by(Title) %>%
  summarise(CommentTotalScore = sum(Score.y)) %>%
  arrange(desc(CommentTotalScore)) %>%
  head(10)

#use base R

base3 <- aggregate(Comments$Score, by=list(Comments$PostId,Comments$UserId), FUN = sum)
colnames(base3)[colnames(base3) == "x"] <- "CommentsTotalScore"
colnames(base3)[colnames(base3) == "Group.1"] <- "Id"
colnames(base3)[colnames(base3) == "Group.2"] <- "OwnerUserId"
base32 <- merge(base3, Posts, by=c("Id","OwnerUserId"))
base32 <- base32[base32$PostTypeId==1, c("Title","CommentsTotalScore"),]
base32 <- base32[order(base32$CommentsTotalScore, decreasing = T),][1:10,]

#use data table

dt3 <- Comments_[,.(CommentsTotalScore = sum(Score)), by=c("PostId","UserId")]
setnames(dt3, "PostId", "Id")
setnames(dt3, "UserId", "OwnerUserId")
dt31 <- merge.data.table(dt3, Posts_, by=c("Id","OwnerUserId"))
dt31 <- dt31[PostTypeId==1,]
dt31 <- dt31[order(CommentsTotalScore,decreasing = T),c("Title","CommentsTotalScore"),][1:10,]
dt31 <- as.data.frame(dt31)


Result3MicroBenchmark <- microbenchmark(sql3, dplyr3, base32, dt3, times = 1000)




sql4 <- sqldf("SELECT DISTINCT
Users.Id, Users.DisplayName, Users.Reputation, Users.Age, Users.Location
FROM (
SELECT
Name, UserID FROM Badges
WHERE Name IN ( SELECT
Name FROM Badges
WHERE Class=1
GROUP BY Name
HAVING COUNT(*) BETWEEN 2 AND 10
)
AND Class=1
) AS ValuableBadges
JOIN Users ON ValuableBadges.UserId=Users.Id")





NameFromBadges <- Badges %>%
  filter(Class==1)%>%
  group_by(Name)%>%
  summarise(Count =  n())%>%
  filter(Count >= 2 & Count <=10)
AsValueableBadges <- Badges %>% 
  filter(Name%in% NameResult$Name & Class ==1)
dplyr4 <- AsValueableBadges %>%
  inner_join(Users, by = c("UserId"="Id")) %>% 
  select(UserId,DisplayName,Reputation,Age,Location) %>% distinct()


#use base R

Name <- Badges[Badges$Class==1,]
Name <- aggregate(Name$Name,by = list(Name$Name), length)
Name <- Name[Name$x >= 2 & Name$x <= 10,]
ValBadges <- Badges[Badges$Class == 1 & Badges$Name %in% Name$Group.1,]
base4 <- merge(ValBadges, Users, by.x = "UserId", by.y = "Id")
base4 <- unique(base4[,c("UserId","DisplayName","Reputation","Age","Location"),])

#use data.table

Name_ <- Badges_[Class==1,]
Name_ <- Name_[,.N, by = Name]
Name_ <- Name_[N >=2 & N <= 10,]
ValBadges_ <- Badges_[Class == 1 & Name %in% Name_$Name,]
dt4 <- unique(merge.data.table(ValBadges_,Users_,by.x = "UserId", by.y = "Id")[,c("UserId","DisplayName","Reputation","Age","Location"),])
dt4 <- as.data.frame(dt4)


Result4MicroBenchmark <- microbenchmark(sql4, dplyr4, base4, dt4, times = 1000)
  

sql5 <- sqldf("SELECT
Questions.Id,
Questions.Title,
BestAnswers.MaxScore,
Posts.Score AS AcceptedScore, BestAnswers.MaxScore-Posts.Score AS Difference
FROM (
SELECT Id, ParentId, MAX(Score) AS MaxScore FROM Posts
WHERE PostTypeId==2
GROUP BY ParentId
) AS BestAnswers JOIN (
SELECT * FROM Posts
WHERE PostTypeId==1 ) AS Questions
ON Questions.Id=BestAnswers.ParentId
JOIN Posts ON Questions.AcceptedAnswerId=Posts.Id WHERE Difference>50
ORDER BY Difference DESC")

#use base r

Posts[Posts$Id==99720,]
Questions <- Posts[Posts$PostTypeId == 1,]
BestAnswers <- Posts[Posts$PostTypeId == 2,]
BestAnswers <- aggregate(BestAnswers$Score, by = list(BestAnswers$ParentId), max)
colnames(BestAnswers)[colnames(BestAnswers) == "x"] <- "Score"
colnames(BestAnswers)[colnames(BestAnswers) == "Group.1"] <- "ParentId"
BestAnswers <- merge(BestAnswers, Posts, by = c("ParentId","Score"))[,c("Id","ParentId","Score"),]
colnames(BestAnswers)[colnames(BestAnswers) == "Score"] <- "MaxScore"
BAQs <- merge(BestAnswers, Questions, by.x = "ParentId", by.y = "Id")[,c("ParentId","Title","MaxScore","Score","AcceptedAnswerId"),]
colnames(BAQs)[colnames(BAQs) == "ParentId"] <- "Id"
Result52 <- merge(BAQs, Posts, by.x = "AcceptedAnswerId", by.y = "Id")[,c("Id","Title.x","MaxScore","Score.y"),]
Result52['Difference'] <- Result52$MaxScore - Result52$Score.y
Result52 <- Result52[Result52$Difference>50,]
Result52 <- Result52[order(Result52$Difference, decreasing = T),]
colnames(Result52)[colnames(Result52) == "Title.x"] <- "Title"
colnames(Result52)[colnames(Result52) == "Score.y"] <- "AcceptedScore"
  
  
 

dpylr51 <- Posts %>% 
  filter(PostTypeId == 1)
dplyr52 <- Posts %>% 
  filter(PostTypeId == 2) %>% 
  group_by(ParentId) %>% 
  summarise(MaxScore=max(Score))
dplyr52 <- dplyr52 %>% 
  inner_join(Posts, by = c("ParentId"="ParentId","MaxScore"="Score")) %>% 
  select(Id,ParentId,MaxScore)
dplyr53 <- dplyr52 %>% 
  inner_join(dplyr51, by = c("ParentId"="Id")) %>% 
  select(ParentId, Title, MaxScore, Score, AcceptedAnswerId) %>% 
  rename(Id = ParentId) %>%
  inner_join(Posts,by = c("AcceptedAnswerId"="Id")) %>% 
  select(Id, Title.x, MaxScore, Score.y) %>% mutate(Difference = MaxScore - Score.y) %>%
  filter(Difference > 50) %>% arrange(desc(Difference)) %>% rename(AcceptedScore = Score.y, Title = Title.x)


#use data table

Questions_ <- Posts_[PostTypeId == 1,]
BestAnswers_dt <- Posts_[PostTypeId == 2,.(Score = max(Score)), by = ParentId]
BestAnswers_dt <- merge.data.table(BestAnswers_dt, Posts_, by = c("ParentId","Score"))[,c("Id","ParentId","Score"),]
setnames(BestAnswers_dt, "Score", "MaxScore")
BAQs_dt <- merge.data.table(BestAnswers_dt, Questions_, by.x = "ParentId", by.y = "Id")[,c("ParentId","Title","MaxScore","Score","AcceptedAnswerId"),]
setnames(BAQs_dt, "ParentId", "Id")
Result54_dt <- merge.data.table(BAQs_dt, Posts_, by.x = "AcceptedAnswerId", by.y = "Id")[,c("Id","Title.x","MaxScore","Score.y"),]
Result54_dt[,Diffrence:= MaxScore - Score.y]
Result54_dt <- Result54_dt[Diffrence > 50,][order(Diffrence,decreasing = T),]
Result54 <- as.data.frame(Result54_dt)

Result5MicroBenchmark <- microbenchmark(sql5, Result52, dplyr53, Result54, times = 1000)

