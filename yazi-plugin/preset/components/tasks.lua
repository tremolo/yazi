Tasks = {
	_id = "tasks",
}

function Tasks:new(area)
	local me = setmetatable({ _area = area }, { __index = self })
	me:layout()
	return me
end

function Tasks:layout()
	self._area = self._area:pad(ui.Pad(1, 1, 1, 3))
	self._chunks = ui.Layout()
		:direction(ui.Layout.HORIZONTAL)
		:constraints({
			ui.Constraint.Percentage(60),
			ui.Constraint.Percentage(40),
		})
		:split(self._area)
end

function Tasks:reflow() return { self } end

function Tasks:redraw()
	local elements = {}
	local has_statusbar = cx.tasks.summary.transfer_pending > 0 or cx.tasks.summary.transfer_blocked > 0
	if has_statusbar then
		elements[#elements + 1] = self:statusbar()
	end

	for i, snap in ipairs(cx.tasks.snaps) do
		local y = self._area.y + (i - 1) * 3
		local bottom = has_statusbar and (self._area.bottom - 1) or self._area.bottom
		if y >= bottom then
			break
		end

		elements[#elements + 1] = ui.Line({ self:icon(snap), snap.name }):area(ui.Rect {
			x = self._area.x,
			y = y,
			w = self._area.w,
			h = 1,
		})

		if i == cx.tasks.cursor + 1 then
			elements[#elements] = elements[#elements]:style(th.tasks.hovered)
		end

		for _, e in ipairs(self:progress_redraw(snap, y + 1)) do
			elements[#elements + 1] = e
		end

		elements[#elements + 1] = ui.Bar(ui.Edge.LEFT)
			:area(ui.Rect {
				x = math.max(0, self._area.x - 2),
				y = y,
				w = self._area.w,
				h = 2,
			})
			:symbol("┃")

		if i == cx.tasks.cursor + 1 then
			elements[#elements] = elements[#elements]:style(th.tasks.hovered)
		end
	end

	return elements
end

function Tasks:statusbar()
	local summary = cx.tasks.summary
	local state = summary.transfer_active and "active" or "idle"
	local text = string.format(
		"Transfer queue: %s | pending: %d | waiting for space: %d",
		state,
		summary.transfer_pending,
		summary.transfer_blocked
	)

	return ui.Line(text)
		:fg("yellow")
		:area(ui.Rect {
			x = self._area.x,
			y = self._area.bottom - 1,
			w = self._area.w,
			h = 1,
		})
end

function Tasks:icon(snap)
	if snap.prog.kind == "FileCopy" then
		return "  "
	elseif snap.prog.kind == "FileCut" then
		return "  "
	elseif snap.prog.kind == "FileDelete" then
		return "  "
	elseif snap.prog.kind == "FileDownload" then
		return "  "
	elseif snap.prog.kind == "FileUpload" then
		return "  "
	else
		return "  "
	end
end

function Tasks:progress_redraw(snap, y)
	local kind = snap.prog.kind
	if
		kind == "FileCopy"
		or kind == "FileCut"
		or kind == "FileDelete"
		or kind == "FileDownload"
		or kind == "FileUpload"
	then
		local percent
		if snap.cooked then
			percent = "Cleaning…"
		else
			percent = string.format("%3d%%", math.floor(snap.percent))
		end

		local label = string.format(
			"%s - %s / %s",
			percent,
			ya.readable_size(snap.prog.processed_bytes),
			ya.readable_size(snap.prog.total_bytes)
		)

		local style = th.status.progress_normal
		if snap.failed or snap.prog.failed_files > 0 then
			style = th.status.progress_error
		end

		return {
			ui.Gauge()
				:area(ui.Rect { x = self._chunks[1].x, y = y, w = self._chunks[1].w, h = 1 })
				:percent(snap.percent)
				:label(ui.Span(label):style(th.status.progress_label))
				:gauge_style(style),

			ui.Line(string.format("%d/%d", snap.prog.success_files, snap.prog.total_files))
				:fg("gray")
				:area(ui.Rect { x = self._chunks[2].x, y = y, w = self._chunks[2].w, h = 1 })
				:align(ui.Align.RIGHT),
		}
	else
		local text
		if snap.cooked then
			text = "Cleaning…"
		elseif snap.running then
			text = "Running…"
		else
			text = "Failed, press Enter to view log…"
		end
		return {
			ui.Line(text):fg("gray"):area(ui.Rect { x = self._chunks[1].x, y = y, w = self._chunks[1].w, h = 1 }),
		}
	end
end
