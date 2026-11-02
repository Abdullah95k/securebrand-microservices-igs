package parse

import (
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// CommentContext is what each comment needs from its post.
type CommentContext struct {
	MediaPK     int64
	OwnerPK     int64
	PostTakenAt *timestamppb.Timestamp
}

// CommentsResult is one comments page.
type CommentsResult struct {
	Comments []*igv1.Comment
	PageInfo walk.PageInfo
	// NodeNull: the post is gone (the query answered a null media).
	NodeNull bool
}

var commentConnections = []string{"comments_connection", "xdt_api__v1__media__media_id__comments__connection", "edge_media_to_parent_comment"}

// Comments parses a PolarisLoggedOutDesktopWWWPostCommentsPaginationQuery
// answer: top-level comments, newest first.
func Comments(parts []gjson.Result, cc CommentContext) (CommentsResult, walk.Report) {
	res, rep := comments(parts, cc)
	if !res.NodeNull && !res.PageInfo.Found && len(res.Comments) == 0 {
		rep.AddMissing("comments_connection")
	}
	return res, rep
}

func comments(parts []gjson.Result, cc CommentContext) (CommentsResult, walk.Report) {
	var res CommentsResult
	var rep walk.Report
	seen := map[int64]bool{}
	for _, p := range parts {
		if v := p.Get("data.xig_polaris_media"); v.Exists() && v.Type == gjson.Null {
			res.NodeNull = true
		}
		for _, key := range commentConnections {
			for _, conn := range walk.FindAll(p, key) {
				for _, e := range conn.Get("edges").Array() {
					c := comment(e.Get("node"), cc, &rep)
					if c == nil || seen[c.CommentPk] {
						continue
					}
					seen[c.CommentPk] = true
					res.Comments = append(res.Comments, c)
				}
			}
		}
	}
	res.PageInfo = walk.FindPageInfo(parts, commentConnections)
	return res, rep
}

func comment(n gjson.Result, cc CommentContext, rep *walk.Report) *igv1.Comment {
	pk := walk.Int64(walk.First(n, "pk", "id"))
	if pk == 0 {
		rep.AddMissing("comment.pk")
		return nil
	}
	c := &igv1.Comment{
		CommentPk: pk, MediaPk: cc.MediaPK, OwnerPk: cc.OwnerPK, PostTakenAt: cc.PostTakenAt,
		Text: n.Get("text").String(), CreatedAt: walk.Unix(walk.First(n, "created_at", "created_at_utc")),
		Status: igv1.CommentStatus_COMMENT_STATUS_VISIBLE,
	}
	if c.CreatedAt == nil {
		rep.AddOptional("comment.created_at")
	}
	if v := walk.First(n, "comment_like_count", "edge_liked_by.count"); v.Type == gjson.Number {
		x := v.Int()
		c.LikeCount = &x
	}
	if v := n.Get("parent_comment_id"); v.Exists() && v.Type != gjson.Null {
		x := walk.Int64(v)
		c.ParentCommentPk = &x
		c.Depth = 1
	}
	if v := n.Get("child_comment_count"); v.Type == gjson.Number {
		x := v.Int()
		c.ChildCommentCount = &x
	}
	if u := walk.First(n, "user", "owner"); u.IsObject() {
		c.Author = &igv1.Author{
			UserPk: walk.Int64(walk.First(u, "pk", "id")), Username: u.Get("username").String(),
			IsVerified: u.Get("is_verified").Bool(), ProfilePicUrl: u.Get("profile_pic_url").String(),
		}
		c.IsOp = cc.OwnerPK != 0 && c.Author.UserPk == cc.OwnerPK
	} else {
		rep.AddOptional("comment.user")
	}
	return c
}
